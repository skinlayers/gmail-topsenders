package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/time/rate"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	DefaultWorkerCount = 4
	MaxRetries         = 5
	InitialBackoff     = 500 * time.Millisecond
	RequestsPerSec     = 200 // quota is 250 QPS; stay under with margin
	cacheFile          = "senders-cache.json"
)

type SenderCount struct {
	Sender string `json:"sender"`
	Count  int    `json:"count"`
	Size   int64  `json:"size"`
}

type cache struct {
	CreatedAt time.Time        `json:"created_at"`
	Query     string           `json:"query"`
	Counts    map[string]int   `json:"counts"`
	Sizes     map[string]int64 `json:"sizes,omitempty"`
}

type senderData struct {
	sender string
	size   int64
}

func main() {
	workers := flag.Int("workers", DefaultWorkerCount, "number of concurrent workers")
	top := flag.Int("top", 50, "number of top senders to display; ignored when -min is set")
	minCount := flag.Int("min", 0, "show all senders with at least this many emails (overrides -top)")
	output := flag.String("output", "", "write results to this JSON file (e.g. results.json)")
	useCache := flag.Bool("cache", false, "cache results and reuse them on subsequent runs if fresh")
	cacheTTL := flag.Duration("cache-ttl", time.Hour, "how long a cache file is considered fresh (e.g. 1h, 30m)")
	cacheFilePath := flag.String("cache-file", cacheFile, "path to the cache file")
	qps := flag.Int("qps", RequestsPerSec, "maximum Gmail API requests per second (quota is 250 QPS)")
	query := flag.String("query", "", "Gmail search query to filter messages (e.g. \"in:inbox\", \"after:2024/01/01\")")
	sortBy := flag.String("sort-by", "count", "sort results by: count or size")
	flag.Parse()

	if *sortBy != "count" && *sortBy != "size" {
		log.Fatalf("invalid -sort-by value %q: must be \"count\" or \"size\"", *sortBy)
	}

	// Implicitly enable cache if cache-related flags were explicitly set.
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "cache-ttl" || f.Name == "cache-file" {
			*useCache = true
		}
	})

	start := time.Now()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	counts := make(map[string]int)
	sizes := make(map[string]int64)
	var n, total int64

	if *useCache {
		if c, ok := loadCache(*cacheFilePath, *cacheTTL, *query); ok {
			counts = c.Counts
			if c.Sizes != nil {
				sizes = c.Sizes
			}
			for _, v := range counts {
				n += int64(v)
			}
			fmt.Printf("Loaded results from cache (created %s ago).\n", time.Since(c.CreatedAt).Round(time.Second))
		}
	}

	if len(counts) == 0 {
		b, err := os.ReadFile("credentials.json")
		if err != nil {
			log.Fatalf("Unable to read client secret file: %v", err)
		}

		config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
		if err != nil {
			log.Fatalf("Unable to parse client secret file to config: %v", err)
		}
		client := getClient(config)

		srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			log.Fatalf("Unable to retrieve Gmail client: %v", err)
		}

		user := "me"
		fmt.Println("Fetching message IDs from Gmail...")

		// 1. Fetch all message IDs (paginated)
		var messageIDs []string
		pageToken := ""
		for {
			req := srv.Users.Messages.List(user).Fields("nextPageToken", "messages(id)")
			if *query != "" {
				req = req.Q(*query)
			}
			if pageToken != "" {
				req.PageToken(pageToken)
			}

			var r *gmail.ListMessagesResponse
			for retry := range MaxRetries {
				r, err = req.Do()
				if err == nil {
					break
				}
				if !isRetryable(err) || retry == MaxRetries-1 {
					log.Fatalf("\nFailed to retrieve messages: %v", err)
				}
				time.Sleep(InitialBackoff * time.Duration(1<<retry))
			}

			for _, m := range r.Messages {
				messageIDs = append(messageIDs, m.Id)
			}

			fmt.Printf("\rFound %d messages...", len(messageIDs))

			pageToken = r.NextPageToken
			if pageToken == "" {
				break
			}
		}
		fmt.Printf("\nTotal messages found: %d. Starting processing...\n", len(messageIDs))

		// 2. Set up worker pool to fetch email headers concurrently
		total = int64(len(messageIDs))
		limiter := rate.NewLimiter(rate.Limit(*qps), *workers)
		idChan := make(chan string, *workers*100)
		senderChan := make(chan senderData, *workers*10)
		var wg sync.WaitGroup
		var processed atomic.Int64

		// Progress reporter
		progressDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					fmt.Printf("\rProcessed %d/%d messages...", processed.Load(), total)
				case <-progressDone:
					return
				}
			}
		}()

		for range *workers {
			wg.Go(func() {
				for id := range idChan {
					if err := limiter.Wait(ctx); err != nil {
						break // context cancelled (Ctrl+C)
					}

					var msg *gmail.Message
					var fetchErr error

				retryLoop:
					for attempt := range 20 {
						msg, fetchErr = srv.Users.Messages.Get(user, id).Format("metadata").MetadataHeaders("From").Do()
						if fetchErr == nil || !isRetryable(fetchErr) {
							break
						}
						select {
						case <-ctx.Done():
							break retryLoop
						case <-time.After(InitialBackoff * time.Duration(1<<min(attempt, 6))):
						}
					}

					if fetchErr != nil {
						log.Printf("Failed to fetch message %s: %v", id, fetchErr)
						continue
					}

					processed.Add(1)
					for _, header := range msg.Payload.Headers {
						if header.Name == "From" {
							senderChan <- senderData{sender: cleanSender(header.Value), size: msg.SizeEstimate}
							break
						}
					}
				}
			})
		}

		go func(ids []string) {
			for _, id := range ids {
				idChan <- id
			}
			close(idChan)
		}(messageIDs)
		messageIDs = nil //nolint:ineffassign // free memory while workers run

		doneAggregating := make(chan struct{})
		go func() {
			for sd := range senderChan {
				counts[sd.sender]++
				sizes[sd.sender] += sd.size
			}
			close(doneAggregating)
		}()

		wg.Wait()
		close(progressDone)
		close(senderChan)
		<-doneAggregating

		n = processed.Load()

		if *useCache && ctx.Err() == nil {
			saveCache(*cacheFilePath, *query, counts, sizes)
		}
	}

	// 3. Sort and print results
	sortedCounts := sortCounts(counts, sizes, *sortBy)
	displayed := applyLimit(sortedCounts, *top, *minCount)

	sortLabel := ""
	if *sortBy == "size" {
		sortLabel = " BY SIZE"
	}

	var totalSize int64
	for _, sc := range sortedCounts {
		totalSize += sc.Size
	}

	// Pre-render rows and compute column widths (seeded with header label widths).
	type row struct{ rank, sender, count, countPct, size, sizePct string }
	rows := make([]row, len(displayed))
	wRank, wSender, wCount, wCountPct, wSize, wSizePct := 1, len("Sender"), len("Emails"), len("Email %"), len("Size"), len("Size %")
	for i, sc := range displayed {
		sizePct := "N/A"
		if totalSize > 0 {
			sizePct = fmt.Sprintf("%.1f%%", float64(sc.Size)/float64(totalSize)*100)
		}
		r := row{
			rank:     fmt.Sprintf("%d", i+1),
			sender:   sc.Sender,
			count:    fmt.Sprintf("%d", sc.Count),
			countPct: fmt.Sprintf("%.1f%%", float64(sc.Count)/float64(n)*100),
			size:     formatSize(sc.Size),
			sizePct:  sizePct,
		}
		rows[i] = r
		if len(r.rank) > wRank {
			wRank = len(r.rank)
		}
		if len(r.sender) > wSender {
			wSender = len(r.sender)
		}
		if len(r.count) > wCount {
			wCount = len(r.count)
		}
		if len(r.countPct) > wCountPct {
			wCountPct = len(r.countPct)
		}
		if len(r.size) > wSize {
			wSize = len(r.size)
		}
		if len(r.sizePct) > wSizePct {
			wSizePct = len(r.sizePct)
		}
	}

	tableWidth := wRank + 2 + wSender + 2 + wCount + 2 + wCountPct + 2 + wSize + 2 + wSizePct

	var banner string
	if ctx.Err() != nil {
		banner = fmt.Sprintf("INTERRUPTED — partial results (%d/%d messages)", n, total)
	} else if *minCount > 0 {
		banner = fmt.Sprintf("SENDERS WITH >=%d EMAILS%s (%d messages)", *minCount, sortLabel, n)
	} else {
		banner = fmt.Sprintf("TOP %d SENDERS%s (%d messages)", *top, sortLabel, n)
	}
	pad := max((tableWidth-len(banner))/2, 0)
	fmt.Printf("\n%s%s\n", strings.Repeat(" ", pad), banner)

	sep := strings.Repeat("-", wRank) + "  " + strings.Repeat("-", wSender) + "  " +
		strings.Repeat("-", wCount) + "  " + strings.Repeat("-", wCountPct) + "  " +
		strings.Repeat("-", wSize) + "  " + strings.Repeat("-", wSizePct)
	fmt.Printf("%*s  %-*s  %*s  %*s  %*s  %*s\n",
		wRank, "#",
		wSender, "Sender",
		wCount, "Emails",
		wCountPct, "Email %",
		wSize, "Size",
		wSizePct, "Size %",
	)
	fmt.Println(sep)
	for _, r := range rows {
		fmt.Printf("%*s  %-*s  %*s  %*s  %*s  %*s\n",
			wRank, r.rank,
			wSender, r.sender,
			wCount, r.count,
			wCountPct, r.countPct,
			wSize, r.size,
			wSizePct, r.sizePct,
		)
	}
	fmt.Printf("Completed in %s\n", time.Since(start).Round(time.Second))

	if *output != "" {
		f, err := os.OpenFile(*output, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Fatalf("Unable to create output file: %v", err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("Unable to close output file: %v", err)
			}
		}()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(displayed); err != nil {
			log.Fatalf("Unable to write JSON output: %v", err)
		}
		fmt.Printf("Results saved to %s\n", *output)
	}
}

func sortCounts(counts map[string]int, sizes map[string]int64, sortBy string) []SenderCount {
	result := make([]SenderCount, 0, len(counts))
	for k, v := range counts {
		result = append(result, SenderCount{Sender: k, Count: v, Size: sizes[k]})
	}
	sort.Slice(result, func(i, j int) bool {
		if sortBy == "size" {
			return result[i].Size > result[j].Size
		}
		return result[i].Count > result[j].Count
	})
	return result
}

func formatSize(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/gb)
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/mb)
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/kb)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func applyLimit(sorted []SenderCount, top, minCount int) []SenderCount {
	if minCount > 0 {
		for i, sc := range sorted {
			if sc.Count < minCount {
				return sorted[:i]
			}
		}
		return sorted
	}
	if top < len(sorted) {
		return sorted[:top]
	}
	return sorted
}

// isRetryable returns true for transient errors (429, 5xx, network). Permanent
// errors like 403/404 should not be retried, except for quota/rate-limit 403s
// which Google surfaces with reason "rateLimitExceeded" or "userRateLimitExceeded".
// The shared rate limiter should prevent quota 403s; this is a safety net.
func isRetryable(err error) bool {
	if apiErr, ok := err.(*googleapi.Error); ok {
		if apiErr.Code == http.StatusTooManyRequests || apiErr.Code >= 500 {
			return true
		}
		if apiErr.Code == http.StatusForbidden {
			for _, e := range apiErr.Errors {
				reason := strings.ToLower(e.Reason)
				if strings.Contains(reason, "ratelimit") || strings.Contains(reason, "quota") {
					return true
				}
			}
		}
		return false
	}
	return true // treat unknown/network errors as transient
}

var emailRegex = regexp.MustCompile(`([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})`)

func cleanSender(raw string) string {
	match := emailRegex.FindStringSubmatch(raw)
	if len(match) > 1 {
		return strings.ToLower(match[1])
	}
	return strings.ToLower(raw)
}

func getClient(config *oauth2.Config) *http.Client {
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

func randomState() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("Unable to generate OAuth state token: %v", err)
	}
	return hex.EncodeToString(b)
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	if tok, ok := autoOAuthFlow(config); ok {
		return tok
	}
	fmt.Println("Could not open browser automatically — falling back to manual authorization.")
	return manualOAuthFlow(config)
}

// autoOAuthFlow starts a local HTTP server, opens the browser, and captures
// the authorization code from the redirect. Returns false if the browser
// could not be opened or the flow times out (e.g. on a headless server).
func autoOAuthFlow(config *oauth2.Config) (*oauth2.Token, bool) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, false
	}
	port := listener.Addr().(*net.TCPAddr).Port

	c := *config
	c.RedirectURL = fmt.Sprintf("http://localhost:%d", port)

	state := randomState()
	codeCh := make(chan string, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != state {
			http.Error(w, "Invalid state parameter", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Missing code parameter", http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprintln(w, "<html><body><h2>Authorized! You can close this tab and return to the terminal.</h2></body></html>")
		codeCh <- code
	})}
	go srv.Serve(listener) //nolint:errcheck
	defer func() {
		if err := srv.Close(); err != nil {
			log.Printf("Unable to close OAuth HTTP server: %v", err)
		}
	}()

	authURL := c.AuthCodeURL(state, oauth2.AccessTypeOffline)
	if err := openBrowser(authURL); err != nil {
		return nil, false
	}
	fmt.Println("Waiting for authorization in your browser...")

	select {
	case code := <-codeCh:
		tok, err := c.Exchange(context.Background(), code)
		if err != nil {
			return nil, false
		}
		fmt.Println("Authorization successful!")
		return tok, true
	case <-time.After(2 * time.Minute):
		return nil, false
	}
}

func manualOAuthFlow(config *oauth2.Config) *oauth2.Token {
	state := randomState()
	authURL := config.AuthCodeURL(state, oauth2.AccessTypeOffline)
	fmt.Printf("Open this URL in your browser:\n\n%v\n\n", authURL)
	fmt.Println("After granting access, your browser will redirect to a page that won't load.")
	fmt.Println("Paste the full URL from your browser's address bar and press Enter:")

	var input string
	if _, err := fmt.Scan(&input); err != nil {
		log.Fatalf("Unable to read input: %v", err)
	}

	authCode := input
	if u, err := url.Parse(input); err == nil {
		if returnedState := u.Query().Get("state"); returnedState != "" && returnedState != state {
			log.Fatalf("OAuth state mismatch — possible CSRF attack, aborting")
		}
		if code := u.Query().Get("code"); code != "" {
			authCode = code
		}
	}

	tok, err := config.Exchange(context.Background(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token: %v", err)
	}
	return tok
}

func openBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Unable to close token file: %v", err)
		}
	}()
	if err := json.NewEncoder(f).Encode(token); err != nil {
		log.Fatalf("Unable to write token: %v", err)
	}
}

func loadCache(path string, ttl time.Duration, query string) (cache, bool) {
	f, err := os.Open(path)
	if err != nil {
		return cache{}, false
	}
	defer f.Close() //nolint:errcheck
	var c cache
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return cache{}, false
	}
	if time.Since(c.CreatedAt) > ttl {
		return cache{}, false
	}
	if c.Query != query {
		return cache{}, false
	}
	return c, true
}

func saveCache(path string, query string, counts map[string]int, sizes map[string]int64) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("Unable to write cache: %v", err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Unable to close cache file: %v", err)
		}
	}()
	if err := json.NewEncoder(f).Encode(cache{CreatedAt: time.Now(), Query: query, Counts: counts, Sizes: sizes}); err != nil {
		log.Printf("Unable to write cache: %v", err)
	}
}
