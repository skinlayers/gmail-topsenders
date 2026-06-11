package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
)

func TestCleanSender(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"user@example.com", "user@example.com"},
		{"John Doe <john@example.com>", "john@example.com"},
		{"USER@EXAMPLE.COM", "user@example.com"},
		{`"John Doe" <john@example.com>`, "john@example.com"},
		{"just a name", "just a name"},
		{"", ""},
	}
	for _, tt := range tests {
		got := cleanSender(tt.input)
		if got != tt.want {
			t.Errorf("cleanSender(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "429 Too Many Requests",
			err:  &googleapi.Error{Code: http.StatusTooManyRequests},
			want: true,
		},
		{
			name: "500 Internal Server Error",
			err:  &googleapi.Error{Code: http.StatusInternalServerError},
			want: true,
		},
		{
			name: "503 Service Unavailable",
			err:  &googleapi.Error{Code: http.StatusServiceUnavailable},
			want: true,
		},
		{
			name: "403 rateLimitExceeded",
			err: &googleapi.Error{
				Code:   http.StatusForbidden,
				Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}},
			},
			want: true,
		},
		{
			name: "403 userRateLimitExceeded",
			err: &googleapi.Error{
				Code:   http.StatusForbidden,
				Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded"}},
			},
			want: true,
		},
		{
			name: "403 no rate limit reason",
			err:  &googleapi.Error{Code: http.StatusForbidden},
			want: false,
		},
		{
			name: "404 Not Found",
			err:  &googleapi.Error{Code: http.StatusNotFound},
			want: false,
		},
		{
			name: "network error",
			err:  errors.New("connection reset by peer"),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRetryable(tt.err)
			if got != tt.want {
				t.Errorf("isRetryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestCacheRoundTrip(t *testing.T) {
	f, err := os.CreateTemp("", "cache-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name()) //nolint:errcheck

	counts := map[string]int{"a@example.com": 10, "b@example.com": 5}
	saveCache(f.Name(), counts)

	c, ok := loadCache(f.Name(), time.Hour)
	if !ok {
		t.Fatal("expected cache to load successfully")
	}
	for k, v := range counts {
		if c.Counts[k] != v {
			t.Errorf("counts[%q] = %d, want %d", k, c.Counts[k], v)
		}
	}
}

func TestCacheTTLExpired(t *testing.T) {
	f, err := os.CreateTemp("", "cache-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name()) //nolint:errcheck

	expired := cache{
		CreatedAt: time.Now().Add(-2 * time.Hour),
		Counts:    map[string]int{"a@example.com": 1},
	}
	if err := json.NewEncoder(f).Encode(expired); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	_, ok := loadCache(f.Name(), time.Hour)
	if ok {
		t.Error("expected expired cache to return false")
	}
}

func TestCacheMissingFile(t *testing.T) {
	_, ok := loadCache("/nonexistent/path/cache.json", time.Hour)
	if ok {
		t.Error("expected missing file to return false")
	}
}

func TestCacheCorruptFile(t *testing.T) {
	f, err := os.CreateTemp("", "cache-*.json")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(name) //nolint:errcheck

	if err := os.WriteFile(name, []byte("not valid json"), 0600); err != nil {
		t.Fatal(err)
	}

	_, ok := loadCache(name, time.Hour)
	if ok {
		t.Error("expected corrupt file to return false")
	}
}
