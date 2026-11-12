package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func metricSamples(t *testing.T, body string) map[string]float64 {
	t.Helper()
	values := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		index := strings.LastIndexByte(line, ' ')
		if index < 0 {
			t.Fatal("invalid metric", line)
		}
		value, err := strconv.ParseFloat(line[index+1:], 64)
		if err != nil {
			t.Fatal(err)
		}
		values[line[:index]] = value
	}
	return values
}

func statsdSamples(t *testing.T, datagrams []string) map[string][]float64 {
	t.Helper()
	values := map[string][]float64{}
	for _, datagram := range datagrams {
		for _, line := range strings.Split(datagram, "\n") {
			index := strings.LastIndexByte(line, ':')
			if index < 0 || !strings.HasSuffix(line, "|g") {
				t.Fatal("invalid StatsD metric", line)
			}
			value, err := strconv.ParseFloat(strings.TrimSuffix(line[index+1:], "|g"), 64)
			if err != nil {
				t.Fatal(err)
			}
			values[line[:index]] = append(values[line[:index]], value)
		}
	}
	return values
}

func TestPostgresMetricsMatchPython(t *testing.T) {
	corpus := loadHousekeepingCorpus(t)
	database := os.Getenv("LOWMEM_TEST_POSTGRES_URL")
	if database == "" {
		t.Skip("set LOWMEM_TEST_POSTGRES_URL to an isolated PostgreSQL database")
	}
	for _, sample := range corpus.Metrics {
		t.Run(sample.Kind, func(t *testing.T) {
			address, err := net.ResolveUDPAddr("udp", "localhost:0")
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.ListenUDP("udp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			gatewayBodies := make(chan string, 2)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Path != "/metrics/job/celery_pg_table_cache_hit_rate" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				body, _ := io.ReadAll(r.Body)
				gatewayBodies <- string(body)
			}))
			defer gateway.Close()
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			defer client.Close()
			ctx := context.Background()
			udp := listener.LocalAddr().(*net.UDPAddr)
			config := housekeepingConfig{PostgresURL: database, PGCacheSQL: sample.SQL, PGTimingSQL: sample.SQL, StatsdHost: udp.IP.String(), StatsdPort: udp.Port, StatsdPrefix: "fixture", StatsdSeparator: "_", Gateway: gateway.URL, ProgressPrefix: "query_async", ProgressTTL: 1200, HeartbeatTTL: 60, PollSeconds: 2, SingletonKey: "poll"}
			h, err := newHousekeeping(ctx, client, config, "lease", "owner")
			if err != nil {
				t.Fatal(err)
			}
			defer h.close()
			connection, err := h.pg.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Release()
			if sample.Kind == "pg_timing" {
				_, err = connection.Exec(ctx, "CREATE TEMP TABLE pg_stat_statements (query text, total_time float8, min_time float8, max_time float8, stddev_time float8, calls bigint, rows bigint)")
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range sample.Rows {
					var queryType *string
					json.Unmarshal(row[0], &queryType)
					query := "plugin-server without a type"
					if queryType != nil {
						query = "/* plugin-server:" + *queryType + " */ SELECT 1"
					}
					var timings [5]float64
					for i := range timings {
						json.Unmarshal(row[i+1], &timings[i])
					}
					var calls, rows int64
					json.Unmarshal(row[6], &calls)
					json.Unmarshal(row[7], &rows)
					_, err = connection.Exec(ctx, "INSERT INTO pg_stat_statements VALUES ($1,$2,$3,$4,$5,$6,$7)", query, timings[0], timings[2], timings[3], timings[4], calls, rows)
					if err != nil {
						t.Fatal(err)
					}
				}
			} else {
				_, err = connection.Exec(ctx, "CREATE TEMP TABLE pg_statio_user_tables (relname text, heap_blks_hit bigint, heap_blks_read bigint)")
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range sample.Rows {
					var name string
					var ratio *float64
					json.Unmarshal(row[0], &name)
					json.Unmarshal(row[1], &ratio)
					var hits, reads int64
					if ratio != nil {
						hits = int64(*ratio * 10)
						reads = 1000 - hits
					}
					_, err = connection.Exec(ctx, "INSERT INTO pg_statio_user_tables VALUES ($1,$2,$3)", name, hits, reads)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			connection.Release()
			if sample.Kind == "pg_timing" {
				err = h.pgTiming(ctx)
			} else {
				err = h.pgCache(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			var datagrams []string
			listener.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			for {
				buffer := make([]byte, 4096)
				length, _, err := listener.ReadFromUDP(buffer)
				if err != nil {
					break
				}
				datagrams = append(datagrams, string(buffer[:length]))
			}
			if !reflect.DeepEqual(statsdSamples(t, datagrams), statsdSamples(t, sample.Statsd)) {
				t.Fatalf("StatsD differs: Go %v; Python %v", datagrams, sample.Statsd)
			}
			body := ""
			select {
			case body = <-gatewayBodies:
			default:
			}
			if !reflect.DeepEqual(metricSamples(t, body), metricSamples(t, sample.Prometheus)) {
				t.Fatalf("Pushgateway differs: Go %s; Python %s", body, sample.Prometheus)
			}
		})
	}
}
