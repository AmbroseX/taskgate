package taskgate

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestCanTransition 七态×七态全枚举:合法清单照 data-model.md,其余全部必须非法。
func TestCanTransition(t *testing.T) {
	legal := map[Status]map[Status]bool{
		StatusBlocked:  {StatusPending: true, StatusCanceled: true},
		StatusPending:  {StatusRunning: true, StatusCanceled: true},
		StatusRunning:  {StatusCompleted: true, StatusFailed: true, StatusRetrying: true, StatusCanceled: true, StatusPending: true},
		StatusRetrying: {StatusRunning: true, StatusCanceled: true},
		// completed/failed/canceled 是终态,没有任何出边
	}
	legalCount := 0
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			want := legal[from][to]
			got := canTransition(from, to)
			if got != want {
				t.Errorf("canTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
			if want {
				legalCount++
			}
		}
	}
	// 合法流转一共 11 条,数量对不上说明表被误改
	if legalCount != 11 {
		t.Errorf("legal transition count = %d, want 11", legalCount)
	}
	// 未知状态一律非法
	if canTransition(Status("bogus"), StatusPending) {
		t.Error("unknown status should not transition")
	}
}

// TestStatusIsFinal 终态判断。
func TestStatusIsFinal(t *testing.T) {
	finals := map[Status]bool{
		StatusCompleted: true, StatusFailed: true, StatusCanceled: true,
		StatusBlocked: false, StatusPending: false, StatusRunning: false, StatusRetrying: false,
	}
	for s, want := range finals {
		if got := s.IsFinal(); got != want {
			t.Errorf("%s.IsFinal() = %v, want %v", s, got, want)
		}
	}
}

// TestDurationUnmarshalText "10m"/"60s" 能解析,非法串报错。
func TestDurationUnmarshalText(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"10m", 10 * time.Minute, false},
		{"60s", 60 * time.Second, false},
		{"1h30m", 90 * time.Minute, false},
		{"abc", 0, true},
		{"", 0, true},
		{"10", 0, true}, // 裸数字没单位,time.ParseDuration 不认
	}
	for _, c := range cases {
		var d Duration
		err := d.UnmarshalText([]byte(c.in))
		if c.wantErr {
			if err == nil {
				t.Errorf("UnmarshalText(%q): want error, got nil", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("UnmarshalText(%q): unexpected error %v", c.in, err)
			continue
		}
		if time.Duration(d) != c.want {
			t.Errorf("UnmarshalText(%q) = %v, want %v", c.in, time.Duration(d), c.want)
		}
	}
}

// TestDurationMarshalText 序列化后再解析能还原。
func TestDurationMarshalText(t *testing.T) {
	d := Duration(10 * time.Minute)
	b, err := d.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	var back Duration
	if err := back.UnmarshalText(b); err != nil {
		t.Fatalf("roundtrip UnmarshalText(%q): %v", b, err)
	}
	if back != d {
		t.Errorf("roundtrip = %v, want %v", back, d)
	}
}

// TestQueueConfigJSON 确认 json 里能直接写 "10m" 这种时长。
func TestQueueConfigJSON(t *testing.T) {
	var q QueueConfig
	raw := `{"workers": 4, "rps": 2.5, "burst": 3, "lease_ttl": "10m"}`
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if q.Workers != 4 || q.RPS != 2.5 || q.Burst != 3 || time.Duration(q.LeaseTTL) != 10*time.Minute {
		t.Errorf("unexpected QueueConfig: %+v", q)
	}
}

// TestSubmitOptions 选项组合:全给、只给部分、DependsOn 可叠加。
func TestSubmitOptions(t *testing.T) {
	at := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	o := applySubmitOptions(
		WithID("job-1"),
		Delay(5*time.Second),
		RunAt(at),
		MaxRetry(3),
		DependsOn("p1", "p2"),
		DependsOn("p3"),
		IgnoreParentFailure(),
	)
	if o.id != "job-1" {
		t.Errorf("id = %q, want job-1", o.id)
	}
	if o.delay != 5*time.Second {
		t.Errorf("delay = %v, want 5s", o.delay)
	}
	if !o.runAt.Equal(at) {
		t.Errorf("runAt = %v, want %v", o.runAt, at)
	}
	if o.maxRetry != 3 {
		t.Errorf("maxRetry = %d, want 3", o.maxRetry)
	}
	if len(o.dependsOn) != 3 || o.dependsOn[0] != "p1" || o.dependsOn[1] != "p2" || o.dependsOn[2] != "p3" {
		t.Errorf("dependsOn = %v, want [p1 p2 p3]", o.dependsOn)
	}
	if !o.ignoreParentFailure {
		t.Error("ignoreParentFailure should be true")
	}

	// 不给任何选项时全是零值(默认 FailFast、不重试、立即可跑)
	zero := applySubmitOptions()
	if zero.id != "" || zero.delay != 0 || !zero.runAt.IsZero() ||
		zero.maxRetry != 0 || zero.dependsOn != nil || zero.ignoreParentFailure {
		t.Errorf("zero options not zero: %+v", zero)
	}
}

// stubBroker 只为让 Config.Broker 非 nil,方法永远不会被 validate 调到。
type stubBroker struct{ Broker }

// validConfig 一份能通过校验的最小配置,各用例在它基础上改坏一处。
func validConfig() Config {
	return Config{
		Broker: stubBroker{},
		Queues: map[string]QueueConfig{
			"q1": {Workers: 2, RPS: 10, Burst: 5, LeaseTTL: Duration(30 * time.Second)},
		},
	}
}

// TestConfigValidateErrors 各种非法配置都必须报错。
func TestConfigValidateErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"nil broker", func(c *Config) { c.Broker = nil }},
		{"workers zero", func(c *Config) { c.Queues["q1"] = QueueConfig{Workers: 0} }},
		{"workers negative", func(c *Config) { c.Queues["q1"] = QueueConfig{Workers: -1} }},
		{"rps negative", func(c *Config) { c.Queues["q1"] = QueueConfig{Workers: 1, RPS: -0.5} }},
		{"burst negative", func(c *Config) { c.Queues["q1"] = QueueConfig{Workers: 1, Burst: -1} }},
		{"lease ttl negative", func(c *Config) {
			c.Queues["q1"] = QueueConfig{Workers: 1, LeaseTTL: Duration(-time.Second)}
		}},
		{"route target missing without default queue", func(c *Config) {
			c.Routes = map[string]string{"ocr": "nowhere"}
		}},
		{"default queue configured but invalid", func(c *Config) {
			c.DefaultQueue = QueueConfig{Workers: 1, RPS: -1}
			c.Routes = map[string]string{"ocr": "nowhere"}
		}},
		{"default queue non-zero but no workers", func(c *Config) {
			c.DefaultQueue = QueueConfig{RPS: 5}
		}},
		{"lease lost max negative", func(c *Config) { c.LeaseLostMax = -1 }},
		{"throttled max negative", func(c *Config) { c.ThrottledMax = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if err := cfg.validate(); err == nil {
				t.Errorf("validate() = nil, want error")
			}
		})
	}
}

// TestConfigValidateDefaults 零值补默认:LeaseTTL 60s、LeaseLostMax 3、ThrottledMax 100。
func TestConfigValidateDefaults(t *testing.T) {
	cfg := Config{
		Broker: stubBroker{},
		Queues: map[string]QueueConfig{"q1": {Workers: 1}},
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate(): %v", err)
	}
	if got := time.Duration(cfg.Queues["q1"].LeaseTTL); got != 60*time.Second {
		t.Errorf("LeaseTTL default = %v, want 60s", got)
	}
	if cfg.LeaseLostMax != 3 {
		t.Errorf("LeaseLostMax default = %d, want 3", cfg.LeaseLostMax)
	}
	if cfg.ThrottledMax != 100 {
		t.Errorf("ThrottledMax default = %d, want 100", cfg.ThrottledMax)
	}
}

// TestConfigValidateRoutes 路由目标在 Queues 里,或 DefaultQueue 能兜底,都合法。
func TestConfigValidateRoutes(t *testing.T) {
	// 目标队列已配置:不需要 DefaultQueue
	cfg := validConfig()
	cfg.Routes = map[string]string{"review": "q1"}
	if err := cfg.validate(); err != nil {
		t.Errorf("route to existing queue: %v", err)
	}

	// 目标队列没配置,但 DefaultQueue 可用:合法,且 DefaultQueue 也补默认
	cfg2 := validConfig()
	cfg2.Routes = map[string]string{"review": "nowhere"}
	cfg2.DefaultQueue = QueueConfig{Workers: 1}
	if err := cfg2.validate(); err != nil {
		t.Fatalf("route falls back to default queue: %v", err)
	}
	if got := time.Duration(cfg2.DefaultQueue.LeaseTTL); got != 60*time.Second {
		t.Errorf("DefaultQueue.LeaseTTL default = %v, want 60s", got)
	}
}

// TestErrThrottled 错误类型能被 errors.As 识别并取出 RetryAfter。
func TestErrThrottled(t *testing.T) {
	var err error = ErrThrottled{RetryAfter: time.Second}
	var et ErrThrottled
	if !errors.As(err, &et) {
		t.Fatal("errors.As should match ErrThrottled")
	}
	if et.RetryAfter != time.Second {
		t.Errorf("RetryAfter = %v, want 1s", et.RetryAfter)
	}
	if err.Error() == "" {
		t.Error("Error() should not be empty")
	}
}

// TestErrSkipRetry Unwrap 能穿透到里面包的业务错误。
func TestErrSkipRetry(t *testing.T) {
	inner := errors.New("bad input")
	var err error = ErrSkipRetry{Err: inner}
	var es ErrSkipRetry
	if !errors.As(err, &es) {
		t.Fatal("errors.As should match ErrSkipRetry")
	}
	if !errors.Is(err, inner) {
		t.Error("errors.Is should unwrap to inner error")
	}
}
