// mockgw_test.go mock 网关自测:每个故障开关的行为 + 并发观测的正确性。
package mockgw_test

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ambrose/taskgate/e2e/mockgw"
)

// fetch 打一次网关,返回状态码和 body 文本;网络层错误(断连)原样返回。
func fetch(t *testing.T, client *http.Client, url, payload string) (int, string, error) {
	t.Helper()
	resp, err := client.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, string(body), nil
}

// TestNormalEcho 默认网关:200 + ok 事件,带 body 回显;观测计数各归各位。
func TestNormalEcho(t *testing.T) {
	gw := mockgw.New()
	defer gw.Close()

	code, body, err := fetch(t, http.DefaultClient, gw.URL(), `{"q":1}`)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("状态码应为 200,得到 %d", code)
	}
	if want := "data: {\"ok\":true,\"echo\":{\"q\":1}}\n\n"; body != want {
		t.Fatalf("响应体应为 %q,得到 %q", want, body)
	}

	// 空 body:不带 echo。
	_, body2, err := fetch(t, http.DefaultClient, gw.URL(), "")
	if err != nil {
		t.Fatalf("第二次请求失败: %v", err)
	}
	if want := "data: {\"ok\":true}\n\n"; body2 != want {
		t.Fatalf("空 body 的响应应为 %q,得到 %q", want, body2)
	}

	if gw.Requests() != 2 || gw.BusyCount() != 0 || gw.CrashCount() != 0 {
		t.Fatalf("计数不对: requests=%d busy=%d crash=%d",
			gw.Requests(), gw.BusyCount(), gw.CrashCount())
	}
	if gw.MaxConcurrency() != 1 {
		t.Fatalf("串行请求 MaxConcurrency 应为 1,得到 %d", gw.MaxConcurrency())
	}
}

// TestLatency 延迟开关:正常请求至少耗时 Latency。
func TestLatency(t *testing.T) {
	gw := mockgw.New(mockgw.Latency(120 * time.Millisecond))
	defer gw.Close()

	start := time.Now()
	if _, _, err := fetch(t, http.DefaultClient, gw.URL(), ""); err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("请求应至少耗时 120ms,实际 %v", elapsed)
	}
}

// TestBusyAfterConcurrency 并发超过 1 → 后到的请求拿到 200 + busy 体,先到的正常。
// 3 个并发:抢到位置的那个占着 2s 延迟(busy 是快速拒绝、立即返回,不吃延迟),
// 另外两个必然落在它的处理窗口内 → 恰好 2 个 busy + 1 个 ok。
// 窗口给足 2s 是为满负载 CI 留余量:goroutine 起跑再慢也慢不过 2s,断言本身保持"恰好"。
func TestBusyAfterConcurrency(t *testing.T) {
	gw := mockgw.New(mockgw.Latency(2*time.Second), mockgw.BusyAfterConcurrency(1))
	defer gw.Close()

	var mu sync.Mutex
	var busyBodies, okBodies int
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body, err := fetch(t, http.DefaultClient, gw.URL(), "")
			if err != nil {
				t.Errorf("请求失败: %v", err)
				return
			}
			if code != http.StatusOK {
				t.Errorf("busy 也必须是 200(错误藏在体里),得到 %d", code)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case strings.Contains(body, `"error":"busy"`):
				busyBodies++
			case strings.Contains(body, `"ok":true`):
				okBodies++
			default:
				t.Errorf("认不出的响应体: %q", body)
			}
		}()
	}
	wg.Wait()

	if busyBodies != 2 || okBodies != 1 {
		t.Fatalf("应恰好 2 busy + 1 ok,实际 busy=%d ok=%d", busyBodies, okBodies)
	}
	if gw.BusyCount() != busyBodies {
		t.Fatalf("BusyCount 应与 busy 响应体一致: 计数=%d 实测=%d", gw.BusyCount(), busyBodies)
	}
}

// TestBusyFirstN 定向开关:前 2 个请求 busy(与并发无关),第 3 个正常。
func TestBusyFirstN(t *testing.T) {
	gw := mockgw.New(mockgw.BusyFirstN(2))
	defer gw.Close()

	for i := 1; i <= 2; i++ {
		code, body, err := fetch(t, http.DefaultClient, gw.URL(), "")
		if err != nil || code != http.StatusOK {
			t.Fatalf("第 %d 次请求异常: code=%d err=%v", i, code, err)
		}
		if !strings.Contains(body, `"error":"busy"`) {
			t.Fatalf("第 %d 次应为 busy 体,得到 %q", i, body)
		}
	}
	_, body, err := fetch(t, http.DefaultClient, gw.URL(), "")
	if err != nil {
		t.Fatalf("第 3 次请求失败: %v", err)
	}
	if !strings.Contains(body, `"ok":true`) {
		t.Fatalf("第 3 次应正常,得到 %q", body)
	}
	if gw.BusyCount() != 2 {
		t.Fatalf("BusyCount 应为 2,得到 %d", gw.BusyCount())
	}
}

// TestFailRateDeterministic 固定种子:失败序列非全零非全一,且同种子两台网关序列完全一致。
func TestFailRateDeterministic(t *testing.T) {
	const n, seed = 40, int64(42)
	run := func() []int {
		gw := mockgw.New(mockgw.FailRate(0.5, seed))
		defer gw.Close()
		codes := make([]int, 0, n)
		for i := 0; i < n; i++ {
			code, _, err := fetch(t, http.DefaultClient, gw.URL(), "")
			if err != nil {
				t.Fatalf("第 %d 次请求失败: %v", i, err)
			}
			codes = append(codes, code)
		}
		return codes
	}

	first := run()
	var fails int
	for _, c := range first {
		if c == http.StatusInternalServerError {
			fails++
		}
	}
	if fails == 0 || fails == n {
		t.Fatalf("p=0.5 打 %d 次,失败数应在 (0,%d) 之间,得到 %d", n, n, fails)
	}

	// 同种子第二台:序列必须逐位一致(CI 确定性)。
	second := run()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("同种子序列第 %d 位不一致: %d vs %d", i, first[i], second[i])
		}
	}
}

// TestCrashAfterConcurrency 并发超过 1 → 后到的连接被直接掐断(客户端看到网络错误),
// 先到的正常完成。3 个并发 → 恰好 2 个断连 + 1 个成功。
// 与 TestBusyAfterConcurrency 同构:2s 窗口给满负载 CI 留余量,断言保持"恰好"。
func TestCrashAfterConcurrency(t *testing.T) {
	gw := mockgw.New(mockgw.Latency(2*time.Second), mockgw.CrashAfterConcurrency(1))
	defer gw.Close()

	var mu sync.Mutex
	var netErrs, oks int
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, _, err := fetch(t, http.DefaultClient, gw.URL(), "")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				netErrs++
				return
			}
			if code == http.StatusOK {
				oks++
			}
		}()
	}
	wg.Wait()

	if netErrs != 2 || oks != 1 {
		t.Fatalf("应恰好 2 个断连 + 1 个成功,实际 断连=%d 成功=%d", netErrs, oks)
	}
	if gw.CrashCount() != 2 {
		t.Fatalf("CrashCount 应为 2,得到 %d", gw.CrashCount())
	}
}

// TestMaxConcurrencyObservation 并发观测正确性:8 个并发请求全部落在 2s 延迟窗口内,
// 峰值必须恰好观测到 8(原子 CAS 不许少算),总请求数 8。
// "恰好 8"是本用例的意义所在(峰值不许少算),不改成 ≥;用 2s 大窗口保证
// 满负载 CI 下 8 个 goroutine 也一定全数重叠,不产生时序偶发。
func TestMaxConcurrencyObservation(t *testing.T) {
	gw := mockgw.New(mockgw.Latency(2 * time.Second))
	defer gw.Close()

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := fetch(t, http.DefaultClient, gw.URL(), ""); err != nil {
				t.Errorf("请求失败: %v", err)
			}
		}()
	}
	wg.Wait()

	if gw.Requests() != n {
		t.Fatalf("总请求数应为 %d,得到 %d", n, gw.Requests())
	}
	if got := gw.MaxConcurrency(); got != n {
		t.Fatalf("峰值并发应观测到 %d,得到 %d", n, got)
	}
}
