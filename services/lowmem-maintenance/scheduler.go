package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type scheduleSpec struct {
	Kind        string  `json:"kind"`
	Seconds     float64 `json:"seconds"`
	Minute      []int   `json:"minute"`
	Hour        []int   `json:"hour"`
	DayOfWeek   []int   `json:"day_of_week"`
	DayOfMonth  []int   `json:"day_of_month"`
	MonthOfYear []int   `json:"month_of_year"`
}

type scheduledTask struct {
	Name       string          `json:"name"`
	Enabled    bool            `json:"enabled"`
	Schedule   scheduleSpec    `json:"schedule"`
	Queue      string          `json:"queue"`
	Message    json.RawMessage `json:"message"`
	Expires    *float64        `json:"expires"`
	Definition string          `json:"definition"`
}

type schedulerConfig struct {
	Version     int             `json:"version"`
	Prefix      string          `json:"prefix"`
	LockKey     string          `json:"lock_key"`
	LockSeconds int             `json:"lock_seconds"`
	Entries     []scheduledTask `json:"entries"`
}

type beatDatetime struct {
	Type        string `json:"__type__"`
	Year        int    `json:"year"`
	Month       int    `json:"month"`
	Day         int    `json:"day"`
	Hour        int    `json:"hour"`
	Minute      int    `json:"minute"`
	Second      int    `json:"second"`
	Microsecond int    `json:"microsecond"`
	Timezone    any    `json:"timezone"`
}

type beatMeta struct {
	LastRun *beatDatetime `json:"last_run_at"`
	Count   int64         `json:"total_run_count"`
}

func newMeta(now time.Time, count int64) string {
	now = now.UTC()
	data, _ := json.Marshal(beatMeta{&beatDatetime{
		"datetime", now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond() / 1000, "UTC",
	}, count})
	return string(data)
}

func (m beatMeta) lastRun() (time.Time, error) {
	if m.LastRun == nil {
		return time.Time{}, nil
	}
	d := m.LastRun
	if d.Timezone != "UTC" && d.Timezone != float64(0) {
		return time.Time{}, errors.New("stored schedule must use UTC")
	}
	return time.Date(d.Year, time.Month(d.Month), d.Day, d.Hour, d.Minute, d.Second, d.Microsecond*1000, time.UTC), nil
}

func contains(values []int, value int) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func (s scheduleSpec) next(last time.Time) (time.Time, error) {
	if s.Kind == "interval" && s.Seconds > 0 {
		return last.Add(time.Duration(s.Seconds * float64(time.Second))), nil
	}
	if s.Kind != "cron" || len(s.Minute) == 0 || len(s.Hour) == 0 || len(s.DayOfWeek) == 0 || len(s.DayOfMonth) == 0 || len(s.MonthOfYear) == 0 {
		return time.Time{}, errors.New("invalid schedule")
	}
	// Celery combines day-of-month and day-of-week with AND, unlike Unix cron.
	start := last.UTC().Truncate(time.Minute).Add(time.Minute)
	end := start.AddDate(400, 0, 0)
	for day := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC); day.Before(end); day = day.AddDate(0, 0, 1) {
		if !contains(s.MonthOfYear, int(day.Month())) || !contains(s.DayOfMonth, day.Day()) || !contains(s.DayOfWeek, int(day.Weekday())) {
			continue
		}
		for _, hour := range s.Hour {
			for _, minute := range s.Minute {
				next := day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
				if !next.Before(start) {
					return next, nil
				}
			}
		}
	}
	return time.Time{}, errors.New("cron has no occurrence in the Gregorian calendar cycle")
}

func (t scheduledTask) messageAt(now time.Time) ([]byte, error) {
	var message struct {
		Body            string         `json:"body"`
		ContentEncoding string         `json:"content-encoding"`
		ContentType     string         `json:"content-type"`
		Headers         map[string]any `json:"headers"`
		Properties      map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(t.Message, &message); err != nil {
		return nil, err
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	id[6], id[8] = (id[6]&0x0f)|0x40, (id[8]&0x3f)|0x80
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	message.Headers["id"] = uuid
	message.Headers["root_id"] = uuid
	message.Properties["correlation_id"] = uuid
	message.Properties["delivery_tag"] = uuid
	if t.Expires != nil {
		message.Headers["expires"] = now.UTC().Add(time.Duration(*t.Expires * float64(time.Second))).Format("2006-01-02T15:04:05.000000+00:00")
	}
	return json.Marshal(message)
}

var renewLease = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`)

var releaseLease = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
`)

var reconcileSchedule = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local names = cjson.decode(ARGV[2])
local entries = cjson.decode(ARGV[3])
local old = redis.call('SMEMBERS', KEYS[2])
for _, name in ipairs(old) do
  if not names[name] then
    redis.call('DEL', ARGV[4] .. name)
    redis.call('ZREM', KEYS[3], ARGV[4] .. name)
    redis.call('SREM', KEYS[2], name)
  end
end
for _, entry in ipairs(entries) do
  local key = ARGV[4] .. entry.name
  redis.call('HSET', key, 'definition', entry.definition)
  redis.call('HSETNX', key, 'meta', ARGV[5])
  redis.call('ZADD', KEYS[3], ARGV[6], key)
  redis.call('SADD', KEYS[2], entry.name)
end
return 1
`)

// Publish and reserve are one transaction. A lost response cannot duplicate a
// dispatch on retry, and a scheduler that lost its lease cannot publish at all.
var dispatchTask = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('HGET', KEYS[2], 'meta') ~= ARGV[2] then return 0 end
redis.call('LPUSH', KEYS[3], ARGV[3])
redis.call('HSET', KEYS[2], 'meta', ARGV[4])
redis.call('ZADD', KEYS[4], ARGV[5], KEYS[2])
return 1
`)

func (c schedulerConfig) reconcile(ctx context.Context, client *redis.Client, token string, now time.Time) error {
	names := make(map[string]bool, len(c.Entries))
	for _, task := range c.Entries {
		names[task.Name] = true
	}
	// Dynamic RedBeat entries are deliberately rejected rather than silently lost.
	keys, err := client.ZRange(ctx, c.Prefix+":schedule", 0, -1).Result()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if len(key) < len(c.Prefix) {
			return errors.New("unexpected RedBeat entry key")
		}
		if !names[key[len(c.Prefix):]] {
			static, err := client.SIsMember(ctx, c.Prefix+":statics", key[len(c.Prefix):]).Result()
			if err != nil || !static {
				return errors.New("dynamic RedBeat schedules require Python beat")
			}
		}
	}
	namesJSON, _ := json.Marshal(names)
	entriesJSON, _ := json.Marshal(append([]scheduledTask{}, c.Entries...))
	result, err := reconcileSchedule.Run(ctx, client, []string{c.LockKey, c.Prefix + ":statics", c.Prefix + ":schedule"}, token, string(namesJSON), string(entriesJSON), c.Prefix, newMeta(now, 0), now.Unix()).Int()
	if err != nil || result != 1 {
		return errors.New("schedule reconciliation failed")
	}
	return nil
}

func (c schedulerConfig) tick(ctx context.Context, client *redis.Client, token string, now time.Time) (time.Duration, error) {
	delay := time.Duration(c.LockSeconds) * time.Second / 3
	owned, err := renewLease.Run(ctx, client, []string{c.LockKey}, token, c.LockSeconds*1000).Int()
	if err != nil || owned != 1 {
		return 0, errors.New("scheduler lease lost")
	}
	pipe := client.Pipeline()
	meta := make([]*redis.StringCmd, len(c.Entries))
	for i, task := range c.Entries {
		meta[i] = pipe.HGet(ctx, c.Prefix+task.Name, "meta")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, errors.New("cannot read run history")
	}
	for i, task := range c.Entries {
		if !task.Enabled {
			continue
		}
		var state beatMeta
		if err := json.Unmarshal([]byte(meta[i].Val()), &state); err != nil {
			return 0, errors.New("invalid run history")
		}
		last, err := state.lastRun()
		if err != nil {
			return 0, err
		}
		due := last.IsZero()
		if !due {
			next, err := task.Schedule.next(last)
			if err != nil {
				return 0, err
			}
			due = !now.Before(next)
			if !due && next.Sub(now) < delay {
				delay = next.Sub(now)
			}
		}
		if !due {
			continue
		}
		message, err := task.messageAt(now)
		if err != nil {
			return 0, err
		}
		next, err := task.Schedule.next(now)
		if err != nil {
			return 0, err
		}
		if next.Sub(now) < delay {
			delay = next.Sub(now)
		}
		result, err := dispatchTask.Run(ctx, client, []string{c.LockKey, c.Prefix + task.Name, task.Queue, c.Prefix + ":schedule"}, token, meta[i].Val(), string(message), newMeta(now, state.Count+1), next.Unix()).Int()
		if err != nil || result < 0 {
			return 0, errors.New("scheduler dispatch failed")
		}
	}
	return delay, nil
}

func runScheduler(ctx context.Context, client *redis.Client, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cannot read exported schedule")
	}
	var config schedulerConfig
	if err := json.Unmarshal(data, &config); err != nil || config.Version != 1 || config.LockKey == "" || config.Prefix == "" || config.LockSeconds < 5 || len(config.Entries) == 0 {
		return errors.New("invalid exported schedule")
	}
	for _, task := range config.Entries {
		if task.Name == "" || task.Queue == "" || !json.Valid(task.Message) || !json.Valid([]byte(task.Definition)) {
			return errors.New("invalid exported entry")
		}
		if _, err := task.Schedule.next(time.Now()); err != nil {
			return err
		}
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return err
	}
	token := fmt.Sprintf("%x", tokenBytes)
	leader := false
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		releaseLease.Run(cleanup, client, []string{config.LockKey}, token)
	}()
	for {
		started := time.Now()
		delay := time.Second
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if !leader {
			leader, err = client.SetNX(callCtx, config.LockKey, token, time.Duration(config.LockSeconds)*time.Second).Result()
			if err == nil && leader {
				err = config.reconcile(callCtx, client, token, time.Now())
				if err == nil {
					log.Printf("scheduler acquired lease; entries=%d", len(config.Entries))
				}
			}
		}
		if err == nil && leader {
			delay, err = config.tick(callCtx, client, token, time.Now())
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			// Exit on uncertainty. A supervisor can restart and resume persisted state.
			return errors.New("scheduler stopped; lease, state or broker operation failed")
		}
		delay -= time.Since(started)
		if delay < time.Millisecond {
			delay = time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
