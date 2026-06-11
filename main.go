package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
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
	cacheFile          = "counts-cache.json"
)

type SenderCount struct {
	Sender string `json:"sender"`
	Count  int    `json:"count"`
}

type cache struct {
	CreatedAt time.Time      `json:"created_at"`
	Counts    map[string]int `json:"counts"`
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
	flag.Parse()

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
	var n, total int64

	if *useCache {
		if c, ok := loadCache(*cacheFilePath, *cacheTTL); ok {
			counts = c.Counts
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
		senderChan := make(chan string, *workers*10)
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
							senderChan <- cleanSender(header.Value)
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
		messageIDs = nil

		doneAggregating := make(chan struct{})
		go func() {
			for sender := range senderChan {
				counts[sender]++
			}
			close(doneAggregating)
		}()

		wg.Wait()
		close(progressDone)
		close(senderChan)
		<-doneAggregating

		n = processed.Load()

		if *useCache && ctx.Err() == nil {
			saveCache(*cacheFilePath, counts)
		}
	}

	// 3. Sort and print results
	var sortedCounts []SenderCount
	for k, v := range counts {
		sortedCounts = append(sortedCounts, SenderCount{Sender: k, Count: v})
	}

	sort.Slice(sortedCounts, func(i, j int) bool {
		return sortedCounts[i].Count > sortedCounts[j].Count
	})
	if ctx.Err() != nil {
		fmt.Printf("\nInterrupted after %d/%d messages. Partial results:\n", n, total)
	} else if *minCount > 0 {
		fmt.Printf("\n--- SENDERS WITH >=%d EMAILS (%d messages) ---\n", *minCount, n)
	} else {
		fmt.Printf("\n--- TOP %d SENDERS (%d messages) ---\n", *top, n)
	}
	for i, sc := range sortedCounts {
		if *minCount > 0 {
			if sc.Count < *minCount {
				break
			}
		} else if i >= *top {
			break
		}
		fmt.Printf("%d. %s: %d emails (%.1f%%)\n", i+1, sc.Sender, sc.Count, float64(sc.Count)/float64(n)*100)
	}
	fmt.Printf("Completed in %s\n", time.Since(start).Round(time.Second))

	if *output != "" {
		limit := min(len(sortedCounts), *top)
		if *minCount > 0 {
			limit = len(sortedCounts)
			for i, sc := range sortedCounts {
				if sc.Count < *minCount {
					limit = i
					break
				}
			}
		}
		f, err := os.OpenFile(*output, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Fatalf("Unable to create output file: %v", err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(sortedCounts[:limit]); err != nil {
			log.Fatalf("Unable to write JSON output: %v", err)
		}
		fmt.Printf("Results saved to %s\n", *output)
	}
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
	authURL := config.AuthCodeURL(randomState(), oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.Background(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
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
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

func loadCache(path string, ttl time.Duration) (cache, bool) {
	f, err := os.Open(path)
	if err != nil {
		return cache{}, false
	}
	defer f.Close()
	var c cache
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return cache{}, false
	}
	if time.Since(c.CreatedAt) > ttl {
		return cache{}, false
	}
	return c, true
}

func saveCache(path string, counts map[string]int) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("Unable to write cache: %v", err)
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(cache{CreatedAt: time.Now(), Counts: counts})
}
