package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type clickhouseConfig struct {
	URL          string `json:"url"`
	Database     string `json:"database"`
	User         string `json:"user"`
	Password     string `json:"password"`
	PasswordFile string `json:"password_file"`
	CA           string `json:"ca"`
	Verify       bool   `json:"verify"`
	Cluster      string `json:"cluster"`
	SQL          string `json:"sql"`
}

type housekeepingConfig struct {
	Handlers        map[string]string `json:"handlers"`
	Queues          []string          `json:"queues"`
	Gateway         string            `json:"gateway"`
	PostgresURL     string            `json:"postgres_url"`
	PGCacheSQL      string            `json:"pg_cache_sql"`
	PGTimingSQL     string            `json:"pg_timing_sql"`
	StatsdHost      string            `json:"statsd_host"`
	StatsdPort      int               `json:"statsd_port"`
	StatsdPrefix    string            `json:"statsd_prefix"`
	StatsdSeparator string            `json:"statsd_separator"`
	Clickhouse      clickhouseConfig  `json:"clickhouse"`
	ProgressPrefix  string            `json:"progress_prefix"`
	ProgressTTL     int               `json:"progress_ttl"`
	HeartbeatTTL    int               `json:"heartbeat_ttl"`
	PollSeconds     float64           `json:"poll_seconds"`
	SingletonKey    string            `json:"singleton_key"`
}

type housekeeping struct {
	maintenance
	config     housekeepingConfig
	pg         *pgxpool.Pool
	clickhouse *http.Client
	statsd     net.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	wait       sync.WaitGroup
	active     map[string]chan struct{}
	lockKey    string
	token      string
}

func newHousekeeping(ctx context.Context, client *redis.Client, config housekeepingConfig, lockKey, token string) (*housekeeping, error) {
	if config.ProgressPrefix == "" || config.SingletonKey == "" || config.PollSeconds <= 0 || config.ProgressTTL <= 0 || config.HeartbeatTTL <= 0 {
		return nil, errors.New("invalid housekeeping configuration")
	}
	pgConfig, err := pgxpool.ParseConfig(config.PostgresURL)
	if err != nil {
		return nil, errors.New("invalid housekeeping PostgreSQL configuration")
	}
	pgConfig.MaxConns, pgConfig.MinConns = 1, 0
	pgConfig.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		return nil, errors.New("cannot initialize housekeeping PostgreSQL pool")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	chTransport := transport.Clone()
	chTransport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !config.Clickhouse.Verify}
	if config.Clickhouse.CA != "" {
		pem, err := os.ReadFile(config.Clickhouse.CA)
		if err != nil {
			pool.Close()
			return nil, errors.New("cannot read ClickHouse CA")
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			pool.Close()
			return nil, errors.New("invalid ClickHouse CA")
		}
		chTransport.TLSClientConfig.RootCAs = roots
	}
	gateway := strings.TrimRight(config.Gateway, "/")
	if gateway != "" && !strings.Contains(gateway, "://") {
		gateway = "http://" + gateway
	}
	ctx, cancel := context.WithCancel(ctx)
	h := &housekeeping{
		maintenance: maintenance{client, config.Queues, gateway, &http.Client{Transport: transport, Timeout: 3 * time.Second}},
		config:      config, pg: pool, clickhouse: &http.Client{Transport: chTransport, Timeout: 10 * time.Second},
		ctx: ctx, cancel: cancel, active: map[string]chan struct{}{}, lockKey: lockKey, token: token,
	}
	for _, kind := range config.Handlers {
		switch kind {
		case "heartbeat", "queue_depth", "pg_cache", "pg_timing", "query_watchdog":
			h.active[kind] = make(chan struct{}, 1)
		default:
			h.close()
			return nil, errors.New("unknown native housekeeping task")
		}
	}
	if config.StatsdHost != "" {
		address := net.JoinHostPort(config.StatsdHost, strconv.Itoa(config.StatsdPort))
		connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "udp", address)
		if err != nil {
			h.close()
			return nil, errors.New("cannot initialize StatsD")
		}
		h.statsd = connection
	}
	h.wait.Add(1)
	go func() { defer h.wait.Done(); h.pollLoop() }()
	return h, nil
}

func (h *housekeeping) close() {
	h.cancel()
	h.wait.Wait()
	if h.statsd != nil {
		h.statsd.Close()
	}
	h.pg.Close()
	h.http.CloseIdleConnections()
	h.clickhouse.CloseIdleConnections()
}

func (h *housekeeping) taskKind(task scheduledTask) string {
	var message struct {
		Headers struct {
			Task string `json:"task"`
		} `json:"headers"`
	}
	if json.Unmarshal(task.Message, &message) != nil {
		return ""
	}
	return h.config.Handlers[message.Headers.Task]
}

func (h *housekeeping) execute(kind string) {
	if kind == "query_watchdog" {
		return
	}
	select {
	case h.active[kind] <- struct{}{}:
	default:
		log.Printf("housekeeping %s still running", kind)
		return
	}
	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		defer func() { <-h.active[kind] }()
		ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
		defer cancel()
		var err error
		switch kind {
		case "heartbeat":
			err = h.heartbeat(ctx)
		case "queue_depth":
			err = h.queueDepth(ctx)
		case "pg_cache":
			err = h.pgCache(ctx)
		case "pg_timing":
			err = h.pgTiming(ctx)
		}
		if err != nil && h.ctx.Err() == nil {
			log.Printf("housekeeping %s failed", kind)
		}
	}()
}

func (h *housekeeping) gauge(name, tag, value string) error {
	if h.statsd == nil {
		return nil
	}
	if h.config.StatsdPrefix != "" {
		name = h.config.StatsdPrefix + h.config.StatsdSeparator + name
	}
	metric := name + "," + tag + ":" + value + "|g"
	if number, err := strconv.ParseFloat(value, 64); err == nil && number < 0 {
		metric = name + "," + tag + ":0|g\n" + metric
	}
	for _, character := range metric {
		if character > 127 {
			return errors.New("StatsD labels must be ASCII")
		}
	}
	h.statsd.SetWriteDeadline(time.Now().Add(time.Second))
	h.statsd.Write([]byte(metric))
	return nil
}

func (h *housekeeping) pgCache(ctx context.Context) error {
	rows, err := h.pg.Query(ctx, h.config.PGCacheSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	var body strings.Builder
	body.WriteString("# HELP posthog_celery_pg_table_cache_hit_rate Postgres query cache hit rate per table.\n# TYPE posthog_celery_pg_table_cache_hit_rate gauge\n")
	for rows.Next() {
		var table string
		var ratio *float64
		if err := rows.Scan(&table, &ratio); err != nil {
			return err
		}
		// Python aborts the registry context on float(None), so no partial
		// Prometheus push is made when a table has no reads yet.
		if ratio == nil {
			return nil
		}
		value := strconv.FormatFloat(*ratio, 'g', -1, 64)
		label := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(table)
		fmt.Fprintf(&body, "posthog_celery_pg_table_cache_hit_rate{table_name=\"%s\"} %s\n", label, value)
		if err := h.gauge("pg_table_cache_hit_rate", "table="+table, value); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if h.gateway == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, h.gateway+"/metrics/job/celery_pg_table_cache_hit_rate", strings.NewReader(body.String()))
	if err != nil {
		return errors.New("invalid metrics gateway")
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response, err := h.http.Do(req)
	if err != nil {
		return errors.New("metrics push failed")
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("metrics push rejected")
	}
	return nil
}

func (h *housekeeping) pgTiming(ctx context.Context) error {
	rows, err := h.pg.Query(ctx, h.config.PGTimingSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var queryType *string
		var timings [5]float64
		var calls, affected int64
		if err := rows.Scan(&queryType, &timings[0], &timings[1], &timings[2], &timings[3], &timings[4], &calls, &affected); err != nil {
			return err
		}
		tag := "query_type=None"
		if queryType != nil {
			tag = "query_type=" + *queryType
		}
		for i, name := range []string{"total_time", "avg_time", "min_time", "max_time", "stddev_time"} {
			if err := h.gauge("pg_plugin_server_query_"+name, tag, strconv.FormatFloat(timings[i], 'g', -1, 64)); err != nil {
				return err
			}
		}
		for name, value := range map[string]int64{"calls": calls, "rows_read_or_affected": affected} {
			if err := h.gauge("pg_plugin_server_query_"+name, tag, strconv.FormatInt(value, 10)); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

type progressInteger string

func (n *progressInteger) UnmarshalJSON(data []byte) error {
	text := string(data)
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
	}
	if strings.EqualFold(text, "nan") {
		*n = "0"
		return nil
	}
	value, _, err := big.ParseFloat(text, 10, 256, big.ToZero)
	if err != nil || value.IsInf() {
		return errors.New("invalid ClickHouse progress number")
	}
	integer, _ := value.Int(nil)
	*n = progressInteger(integer.String())
	return nil
}

func (n progressInteger) MarshalJSON() ([]byte, error) {
	if n == "" {
		return nil, errors.New("missing progress number")
	}
	return []byte(n), nil
}

type queryProgress struct {
	InitialID     string          `json:"initial_query_id"`
	QueryID       string          `json:"query_id"`
	BytesRead     progressInteger `json:"bytes_read"`
	RowsRead      progressInteger `json:"rows_read"`
	EstimatedRows progressInteger `json:"estimated_rows_total"`
	Elapsed       progressInteger `json:"time_elapsed"`
	CPU           progressInteger `json:"active_cpu_time"`
}

func (c clickhouseConfig) password() string {
	if c.PasswordFile == "" {
		return c.Password
	}
	data, err := os.ReadFile(c.PasswordFile)
	if err != nil || strings.TrimSpace(string(data)) == "" {
		return c.Password
	}
	token := strings.TrimSpace(string(data))
	parts := strings.Split(token, ".")
	if c.Password != "" && len(parts) == 3 {
		payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
		if err == nil {
			var claims map[string]json.RawMessage
			if json.Unmarshal(payload, &claims) == nil {
				var expiry *float64
				if json.Unmarshal(claims["exp"], &expiry) == nil && expiry != nil && float64(time.Now().UnixNano())/1e9 >= *expiry-10 {
					return c.Password
				}
			}
		}
	}
	return token
}

func (h *housekeeping) queryProgress(ctx context.Context) ([]queryProgress, error) {
	c := h.config.Clickhouse
	endpoint, err := url.Parse(c.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("invalid ClickHouse URL")
	}
	parameters := endpoint.Query()
	parameters.Set("database", c.Database)
	parameters.Set("param_cluster", c.Cluster)
	parameters.Set("output_format_json_quote_denormals", "1")
	parameters.Set("log_comment", `{"product":"internal","feature":"health_check","name":"poll_query_performance"}`)
	endpoint.RawQuery = parameters.Encode()
	// The maintenance SQL is exported from Python. Only result aliases and
	// output encoding change; analytics query text never enters this service.
	sql := "SELECT initial_query_id, query_id, read_bytes AS bytes_read, read_rows AS rows_read, total_rows_approx AS estimated_rows_total, elapsed AS time_elapsed, OSCPUVirtualTimeMicroseconds AS active_cpu_time FROM (" + c.SQL + ") FORMAT JSONEachRow"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(sql))
	if err != nil {
		return nil, errors.New("invalid ClickHouse request")
	}
	req.SetBasicAuth(c.User, c.password())
	response, err := h.clickhouse.Do(req)
	if err != nil {
		return nil, errors.New("ClickHouse request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("ClickHouse request rejected")
	}
	decoder := json.NewDecoder(response.Body)
	var results []queryProgress
	for {
		var row queryProgress
		if err := decoder.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.New("invalid ClickHouse progress response")
		}
		results = append(results, row)
	}
	return results, nil
}

var initialQuery = regexp.MustCompile(`(?i)^(\d+)_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})`)

var claimProgressPoll = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
return 1
`)

func (h *housekeeping) pollProgress(ctx context.Context) error {
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(time.Now().UnixNano()))
	claimed, err := claimProgressPoll.Run(ctx, h.redis, []string{h.lockKey, h.config.SingletonKey}, h.token, timestamp[:]).Int()
	if err != nil {
		return err
	}
	if claimed != 1 {
		return nil
	}
	results, err := h.queryProgress(ctx)
	if err != nil {
		return err
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].InitialID < results[j].InitialID })
	for start := 0; start < len(results); {
		end := start + 1
		for end < len(results) && results[end].InitialID == results[start].InitialID {
			end++
		}
		match := initialQuery.FindStringSubmatch(results[start].InitialID)
		if match == nil {
			start = end
			continue
		}
		team := strings.TrimLeft(match[1], "0")
		if team == "" {
			team = "0"
		}
		base := h.config.ProgressPrefix + ":" + team + ":" + match[2]
		key := base + ":status"
		group := results[start:end]
		err := h.redis.Watch(ctx, func(tx *redis.Tx) error {
			owner, err := tx.Get(ctx, h.lockKey).Result()
			if err != nil || owner != h.token {
				return errors.New("progress poll lease lost")
			}
			previous, err := tx.Get(ctx, key).Bytes()
			if err != nil && err != redis.Nil {
				return err
			}
			progress := map[string]json.RawMessage{}
			if err != redis.Nil {
				if err := json.Unmarshal(previous, &progress); err != nil || progress == nil {
					return errors.New("invalid cached progress")
				}
			}
			for _, row := range group {
				encoded, err := json.Marshal(row)
				if err != nil {
					return err
				}
				progress[row.QueryID] = encoded
			}
			encoded, err := json.Marshal(progress)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, encoded, time.Duration(h.config.ProgressTTL)*time.Second)
				pipe.Set(ctx, base+":heartbeat", "1", time.Duration(h.config.HeartbeatTTL)*time.Second)
				return nil
			})
			return err
		}, h.lockKey, key)
		if err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (h *housekeeping) pollLoop() {
	for {
		started := time.Now()
		ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
		err := h.pollProgress(ctx)
		cancel()
		if err != nil && h.ctx.Err() == nil {
			log.Print("query progress poll failed")
		}
		delay := time.Duration(h.config.PollSeconds*float64(time.Second)) - time.Since(started)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-h.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
