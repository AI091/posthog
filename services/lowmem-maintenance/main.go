package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type maintenance struct {
	redis   *redis.Client
	queues  []string
	gateway string
	http    *http.Client
}

func (m *maintenance) heartbeat(ctx context.Context) error {
	return m.redis.Set(ctx, "POSTHOG_HEARTBEAT", time.Now().Unix(), 0).Err()
}

func (m *maintenance) queueDepth(ctx context.Context) error {
	pipe := m.redis.Pipeline()
	counts := make([]*redis.IntCmd, len(m.queues))
	for i, queue := range m.queues {
		counts[i] = pipe.LLen(ctx, queue)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.New("queue depth Redis request failed")
	}
	if m.gateway == "" {
		return nil
	}
	var body strings.Builder
	body.WriteString("# HELP posthog_celery_queue_depth We use this to monitor the depth of the celery queue.\n# TYPE posthog_celery_queue_depth gauge\n")
	for i, queue := range m.queues {
		fmt.Fprintf(&body, "posthog_celery_queue_depth{queue_name=%q} %d\n", queue, counts[i].Val())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, m.gateway+"/metrics/job/redis_celery_queue_depth_registry", strings.NewReader(body.String()))
	if err != nil {
		return errors.New("invalid Pushgateway request")
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	resp, err := m.http.Do(req)
	if err != nil {
		return errors.New("Pushgateway request failed")
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Pushgateway returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func runLoop(ctx context.Context, task func(context.Context) error, name string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := task(callCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("%s failed", name)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func run() error {
	opts, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return errors.New("REDIS_URL must be a Redis URL")
	}
	opts.MaxRetries = -1
	opts.DialTimeout = 3 * time.Second
	opts.ReadTimeout = 3 * time.Second
	opts.WriteTimeout = 3 * time.Second
	opts.PoolSize = 2
	opts.ContextTimeoutEnabled = true
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 3 && os.Args[1] == "--schedule" {
		return runScheduler(ctx, client, os.Args[2])
	}
	if len(os.Args) != 1 {
		return errors.New("usage: maintenance [--schedule file]")
	}
	data, err := os.ReadFile("queues.json")
	if err != nil {
		return errors.New("cannot read queues.json")
	}
	var queues []string
	if err := json.Unmarshal(data, &queues); err != nil || len(queues) == 0 {
		return errors.New("queues.json must contain queue names")
	}
	for _, queue := range queues {
		if queue == "" || strings.ContainsAny(queue, "\n\r\\\"") {
			return errors.New("invalid queue name")
		}
	}
	gateway := strings.TrimRight(os.Getenv("PROM_PUSHGATEWAY_ADDRESS"), "/")
	if gateway != "" {
		if !strings.Contains(gateway, "://") {
			gateway = "http://" + gateway
		}
		u, err := url.Parse(gateway)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("invalid Pushgateway URL")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	m := maintenance{client, queues, gateway, &http.Client{Transport: transport, Timeout: 3 * time.Second}}
	var wg sync.WaitGroup
	for name, task := range map[string]func(context.Context) error{"heartbeat": m.heartbeat, "queue depth": m.queueDepth} {
		wg.Add(1)
		go func() { defer wg.Done(); runLoop(ctx, task, name) }()
	}
	wg.Wait()
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
