package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type housekeepingCorpus struct {
	Rows     []json.RawMessage `json:"rows"`
	OldKey   string            `json:"old_key"`
	Old      json.RawMessage   `json:"old"`
	Expected []struct {
		Key          string          `json:"key"`
		Value        json.RawMessage `json:"value"`
		TTL          int             `json:"ttl"`
		Heartbeat    string          `json:"heartbeat"`
		HeartbeatTTL int             `json:"heartbeat_ttl"`
	} `json:"expected"`
	Metrics []struct {
		Kind       string              `json:"kind"`
		Rows       [][]json.RawMessage `json:"rows"`
		SQL        string              `json:"sql"`
		Statsd     []string            `json:"statsd"`
		Prometheus string              `json:"prometheus"`
	} `json:"metrics"`
	ProgressPrefix string `json:"progress_prefix"`
	ProgressTTL    int    `json:"progress_ttl"`
	HeartbeatTTL   int    `json:"heartbeat_ttl"`
	SingletonKey   string `json:"singleton_key"`
	SQL            string `json:"sql"`
}

func loadHousekeepingCorpus(t *testing.T) housekeepingCorpus {
	t.Helper()
	path := os.Getenv("HOUSEKEEPING_PARITY_FILE")
	if path == "" {
		t.Skip("generate the Python contract with test_housekeeping.py")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var corpus housekeepingCorpus
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	return corpus
}

func decodeContractJSON(t *testing.T, data []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestProgressMatchesPython(t *testing.T) {
	corpus := loadHousekeepingCorpus(t)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx := context.Background()
	client.Set(ctx, "lease", "owner", time.Minute)
	if err := client.Set(ctx, corpus.OldKey, []byte(corpus.Old), 30*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(tokenFile, []byte("first-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	var wanted atomic.Value
	wanted.Store("first-token")
	var fence, empty atomic.Bool
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		user, password, ok := request.BasicAuth()
		if !ok || user != "ops" || password != wanted.Load().(string) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(request.Body)
		if request.URL.Query().Get("param_cluster") != "posthog" || !strings.Contains(string(body), corpus.SQL) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fence.Load() {
			client.Set(ctx, "lease", "successor", time.Minute)
		}
		if !empty.Load() {
			for _, row := range corpus.Rows {
				w.Write(row)
				w.Write([]byte("\n"))
			}
		}
	}))
	defer endpoint.Close()
	h := housekeeping{
		maintenance: maintenance{redis: client},
		config:      housekeepingConfig{ProgressPrefix: corpus.ProgressPrefix, ProgressTTL: corpus.ProgressTTL, HeartbeatTTL: corpus.HeartbeatTTL, SingletonKey: corpus.SingletonKey, Clickhouse: clickhouseConfig{URL: endpoint.URL, User: "ops", Password: "fallback", PasswordFile: tokenFile, Cluster: "posthog", SQL: corpus.SQL}},
		clickhouse:  endpoint.Client(), lockKey: "lease", token: "owner",
	}
	if err := h.pollProgress(ctx); err != nil {
		t.Fatal(err)
	}
	for _, expected := range corpus.Expected {
		actual, err := client.Get(ctx, expected.Key).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decodeContractJSON(t, actual), decodeContractJSON(t, expected.Value)) {
			t.Fatalf("progress differs from Python for %s: %s", expected.Key, actual)
		}
		if ttl := server.TTL(expected.Key); ttl < time.Duration(expected.TTL-1)*time.Second || ttl > time.Duration(expected.TTL+1)*time.Second {
			t.Fatal("progress TTL differs", ttl)
		}
		heartbeat := strings.TrimSuffix(expected.Key, ":status") + ":heartbeat"
		if client.Get(ctx, heartbeat).Val() != expected.Heartbeat || server.TTL(heartbeat) != time.Duration(corpus.HeartbeatTTL)*time.Second {
			t.Fatal("heartbeat differs")
		}
	}
	if err := os.WriteFile(tokenFile, []byte("rotated-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	wanted.Store("rotated-token")
	if err := h.pollProgress(ctx); err != nil {
		t.Fatal("credential was not re-read", err)
	}
	server.FastForward(5 * time.Second)
	empty.Store(true)
	if err := h.pollProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if server.TTL(corpus.OldKey) != time.Duration(corpus.ProgressTTL-5)*time.Second {
		t.Fatal("empty result refreshed stale progress")
	}
	empty.Store(false)
	fence.Store(true)
	before := client.Get(ctx, corpus.OldKey).Val()
	if err := h.pollProgress(ctx); err == nil {
		t.Fatal("stale poller must be fenced after the HTTP request")
	}
	if client.Get(ctx, corpus.OldKey).Val() != before {
		t.Fatal("stale poller changed progress")
	}
	if len(client.Get(ctx, corpus.SingletonKey).Val()) != 8 {
		t.Fatal("singleton timestamp is not Python-compatible")
	}
}

func TestRotatingCredentialFallback(t *testing.T) {
	file := filepath.Join(t.TempDir(), "credential")
	config := clickhouseConfig{Password: "fallback", PasswordFile: file}
	if config.password() != "fallback" {
		t.Fatal("missing file must use fallback")
	}
	for _, sample := range []struct {
		claims string
		want   string
	}{
		{`{"exp":1}`, "fallback"},
		{`{"exp":4102444800}`, "file"},
		{`{"exp":null}`, "file"},
		{`{"exp":true}`, "file"},
		{`{"exp":"1"}`, "file"},
		{`{}`, "file"},
	} {
		t.Run(sample.claims, func(t *testing.T) {
			token := "header." + base64.RawURLEncoding.EncodeToString([]byte(sample.claims)) + ".signature"
			if err := os.WriteFile(file, []byte(token), 0o600); err != nil {
				t.Fatal(err)
			}
			want := token
			if sample.want == "fallback" {
				want = "fallback"
			}
			if got := config.password(); got != want {
				t.Fatal("wrong credential selection")
			}
			if sample.want == "fallback" {
				withoutFallback := config
				withoutFallback.Password = ""
				if withoutFallback.password() != token {
					t.Fatal("token-only auth was discarded")
				}
			}
		})
	}
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if config.password() != "fallback" {
		t.Fatal("empty file must use fallback")
	}
}

func TestNativeTaskRetainsScheduleWithoutCeleryPublication(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &housekeeping{maintenance: maintenance{redis: client}, ctx: ctx, active: map[string]chan struct{}{"heartbeat": make(chan struct{}, 1)}, config: housekeepingConfig{Handlers: map[string]string{"heartbeat": "heartbeat"}}}
	config := schedulerConfig{Version: 2, Prefix: "redbeat:", LockKey: "lease", LockSeconds: 45, native: h, Entries: []scheduledTask{{Name: "heartbeat", Enabled: true, Schedule: scheduleSpec{Kind: "interval", Seconds: 10}, Queue: "celery", Definition: `{}`, Message: json.RawMessage(`{"headers":{"task":"heartbeat"}}`)}}}
	now := time.Now()
	client.Set(ctx, "lease", "owner", time.Minute)
	if err := config.reconcile(ctx, client, "owner", now); err != nil {
		t.Fatal(err)
	}
	if _, err := config.tick(ctx, client, "owner", now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	h.wait.Wait()
	if client.Exists(ctx, "POSTHOG_HEARTBEAT").Val() != 1 || client.LLen(ctx, "celery").Val() != 0 {
		t.Fatal("native task missing or also published to Python")
	}
	var meta beatMeta
	json.Unmarshal([]byte(client.HGet(ctx, "redbeat:heartbeat", "meta").Val()), &meta)
	if meta.Count != 1 {
		t.Fatal("native execution was not reserved")
	}
}
