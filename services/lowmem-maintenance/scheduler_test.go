package main

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSchedulerDispatchRestartAndFencing(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expires := 15.0
	config := schedulerConfig{Version: 1, Prefix: "redbeat:", LockKey: "redbeat::lock", LockSeconds: 45, Entries: []scheduledTask{{
		Name: "heartbeat", Enabled: true, Schedule: scheduleSpec{Kind: "interval", Seconds: 10}, Queue: "stats",
		Definition: `{}`, Expires: &expires,
		Message: json.RawMessage(`{"body":"W1tdLHt9LHt9XQ==","content-encoding":"utf-8","content-type":"application/json","headers":{"task":"heartbeat"},"properties":{"priority":0}}`),
	}}}
	disabled := config.Entries[0]
	disabled.Name, disabled.Queue, disabled.Enabled = "disabled", "disabled", false
	config.Entries = append(config.Entries, disabled)
	client.Set(ctx, config.LockKey, "first", 45*time.Second)
	if err := config.reconcile(ctx, client, "first", now); err != nil {
		t.Fatal(err)
	}
	if _, err := config.tick(ctx, client, "first", now.Add(9*time.Second)); err != nil {
		t.Fatal(err)
	}
	if client.LLen(ctx, "stats").Val() != 0 {
		t.Fatal("dispatched early")
	}
	if _, err := config.tick(ctx, client, "first", now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	client.Set(ctx, config.LockKey, "second", 45*time.Second)
	if _, err := config.tick(ctx, client, "first", now.Add(20*time.Second)); err == nil {
		t.Fatal("stale leader must fail")
	}
	if err := config.reconcile(ctx, client, "second", now.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := config.tick(ctx, client, "second", now.Add(19*time.Second)); err != nil {
		t.Fatal(err)
	}
	if client.LLen(ctx, "stats").Val() != 1 {
		t.Fatal("restart duplicated an already published message")
	}
	if _, err := config.tick(ctx, client, "second", now.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if client.LLen(ctx, "stats").Val() != 2 {
		t.Fatal("restart lost the next dispatch")
	}
	if client.LLen(ctx, "disabled").Val() != 0 {
		t.Fatal("disabled schedule was published")
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(client.LIndex(ctx, "stats", 0).Val()), &message); err != nil {
		t.Fatal(err)
	}
	headers := message["headers"].(map[string]any)
	if headers["expires"] != "2026-01-01T00:00:35.000000+00:00" || headers["id"] != message["properties"].(map[string]any)["correlation_id"] {
		t.Fatal("invalid Celery id or expiry", message)
	}
	// Simulate an ambiguous response: repeating the exact transaction cannot push again.
	result, err := dispatchTask.Run(ctx, client, []string{config.LockKey, config.Prefix + "heartbeat", "stats", config.Prefix + ":schedule"}, "second", newMeta(now.Add(10*time.Second), 1), `{}`, newMeta(now.Add(20*time.Second), 2), now.Unix()).Int()
	if err != nil || result != 0 || client.LLen(ctx, "stats").Val() != 2 {
		t.Fatal("dispatch retry duplicated a message", result, err)
	}
}

func TestSchedulerRemovesOldStaticEntries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	ctx := context.Background()
	config := schedulerConfig{Prefix: "redbeat:", LockKey: "redbeat::lock"}
	client.Set(ctx, config.LockKey, "owner", time.Minute)
	client.SAdd(ctx, "redbeat::statics", "removed")
	client.HSet(ctx, "redbeat:removed", "meta", newMeta(time.Now(), 1))
	client.ZAdd(ctx, "redbeat::schedule", redis.Z{Score: 1, Member: "redbeat:removed"})
	if err := config.reconcile(ctx, client, "owner", time.Now()); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, "redbeat:removed").Val() != 0 || client.ZCard(ctx, "redbeat::schedule").Val() != 0 {
		t.Fatal("removed schedule still exists")
	}
	client.ZAdd(ctx, "redbeat::schedule", redis.Z{Score: 1, Member: "redbeat:dynamic"})
	if err := config.reconcile(ctx, client, "owner", time.Now()); err == nil {
		t.Fatal("must not silently omit dynamic schedules")
	}
}

func TestCeleryParity(t *testing.T) {
	path := os.Getenv("CELERY_PARITY_FILE")
	if path == "" {
		t.Skip("generate a parity corpus with test_export_schedule.py")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Config schedulerConfig `json:"config"`
		Cases  []struct {
			Name    string         `json:"name"`
			Last    time.Time      `json:"last"`
			Next    time.Time      `json:"next"`
			Queue   string         `json:"queue"`
			Message map[string]any `json:"message"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	tasks := map[string]scheduledTask{}
	for _, task := range corpus.Config.Entries {
		tasks[task.Name] = task
	}
	for _, sample := range corpus.Cases {
		t.Run(sample.Name+sample.Last.String(), func(t *testing.T) {
			task, ok := tasks[sample.Name]
			if !ok {
				t.Fatal("missing schedule", sample.Name)
			}
			next, err := task.Schedule.next(sample.Last)
			if err != nil || !next.Equal(sample.Next) {
				t.Fatalf("schedule diff: got %s, beat %s (%v)", next, sample.Next, err)
			}
			if task.Queue != sample.Queue {
				t.Fatalf("queue diff: got %q, beat %q", task.Queue, sample.Queue)
			}
			encoded, err := task.messageAt(sample.Last)
			if err != nil {
				t.Fatal(err)
			}
			var message map[string]any
			json.Unmarshal(encoded, &message)
			if expiry := sample.Message["headers"].(map[string]any)["expires"]; expiry != nil {
				milliseconds, err := strconv.ParseFloat(sample.Message["properties"].(map[string]any)["expiration"].(string), 64)
				if err != nil {
					t.Fatal(err)
				}
				got, err := time.Parse(time.RFC3339Nano, message["headers"].(map[string]any)["expires"].(string))
				want := sample.Last.Add(time.Duration(milliseconds * float64(time.Millisecond)))
				if err != nil || !got.Equal(want) {
					t.Fatalf("expiry diff: Go %s, beat duration %s (%v)", got, want, err)
				}
			}
			for _, field := range []string{"id", "root_id", "origin", "expires"} {
				delete(message["headers"].(map[string]any), field)
				delete(sample.Message["headers"].(map[string]any), field)
			}
			for _, field := range []string{"correlation_id", "delivery_tag", "reply_to"} {
				delete(message["properties"].(map[string]any), field)
				delete(sample.Message["properties"].(map[string]any), field)
			}
			if !reflect.DeepEqual(message, sample.Message) {
				t.Fatalf("message diff:\nGo: %s\nbeat: %v", encoded, sample.Message)
			}
		})
	}
	t.Logf("compared %d exported schedules and %d beat publications", len(tasks), len(corpus.Cases))
}
