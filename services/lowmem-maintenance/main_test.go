package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMaintenanceContract(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	defer client.Close()
	server.Push("celery", "one", "two")
	server.Push("stats", "one")
	var got string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" || r.URL.Path != "/metrics/job/redis_celery_queue_depth_registry" {
			t.Errorf("unexpected push: %s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		got = string(b)
	}))
	defer gateway.Close()
	m := maintenance{client, []string{"celery", "stats", "email"}, gateway.URL, gateway.Client()}
	before := time.Now().Unix()
	if err := m.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	heartbeat, err := client.Get(context.Background(), "POSTHOG_HEARTBEAT").Int64()
	if err != nil || heartbeat < before || heartbeat > time.Now().Unix() {
		t.Fatalf("invalid heartbeat: %d %v", heartbeat, err)
	}
	if server.TTL("POSTHOG_HEARTBEAT") != 0 {
		t.Fatal("heartbeat must not expire")
	}
	if err := m.queueDepth(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`queue_name="celery"} 2`, `queue_name="stats"} 1`, `queue_name="email"} 0`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
	items, err := server.List("celery")
	if err != nil || len(items) != 2 || items[0] != "one" {
		t.Fatal("probe changed queue")
	}
}

func TestPushFailureIsReported(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer gateway.Close()
	m := maintenance{client, []string{"celery"}, gateway.URL, gateway.Client()}
	if err := m.queueDepth(context.Background()); err == nil {
		t.Fatal("push failure was hidden")
	}
}

func TestCancellationStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runLoop(ctx, func(context.Context) error { cancel(); return nil }, "test")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
}
