// 多进程原型验证(裁决第 4 步之二,`go test -short` 跳过):
//
//	① 硬上限:双进程共享同一 sqlite 文件、同一 quota key,每窗口合计启动 ≤ N,
//	   窗口键由介质钟在预留语句里原子计算,不存在边界双窗口放量;
//	② 窗口恢复:被完整覆盖的每个窗口都恰好打满 N(额度确实随窗口切换恢复);
//	③ kill -9 泄漏保守:进程在"预留成功、认领未完成"之间被杀,该窗口总放行
//	   恰好 N-1——只少不多;
//	④ fail-closed:介质不可达(长持写锁模拟)期间零放行,恢复后自动继续。
//
// 子进程模式沿用 sqlitebroker/crash_test.go:exec 自身测试二进制 + 环境变量开关
// + 哨兵文件;子进程把每次"handler 启动"写进各自的本地日志(win + 本地毫秒时间),
// 父进程合并断言。窗口归属一律以预留语句返回的 win(介质钟)为准。
package quota

import (
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 子进程环境变量约定。
const (
	envHelper   = "TASKGATE_QP_HELPER"   // "1" 才干活
	envMode     = "TASKGATE_QP_MODE"     // consume | leak
	envDB       = "TASKGATE_QP_DB"       // sqlite 文件路径
	envKey      = "TASKGATE_QP_KEY"      // quota key
	envPeriod   = "TASKGATE_QP_PERIOD"   // 窗口秒数
	envLimit    = "TASKGATE_QP_LIMIT"    // QuotaLimit
	envDur      = "TASKGATE_QP_DUR"      // consume 模式跑多久(Go duration)
	envLog      = "TASKGATE_QP_LOG"      // 本地日志路径
	envSentinel = "TASKGATE_QP_SENTINEL" // leak 模式:预留成功后写哨兵(内容 = win)
	envBusyMS   = "TASKGATE_QP_BUSY"     // busy_timeout 毫秒
)

// TestQuotaProcHelper 不是常规测试:只在被父测试作为子进程拉起时才干活。
//   - consume:认领循环的原型——预留成功记一次"handler 启动"并写日志;
//     耗尽小睡等窗口;介质出错记 err 行、退避重试,期间零放行(fail-closed);
//   - leak:kill -9 的受害者——预留一次,把 win 写进哨兵文件,然后永久卡死。
func TestQuotaProcHelper(t *testing.T) {
	if os.Getenv(envHelper) != "1" {
		t.Skip("仅作为多进程原型的子进程运行")
	}
	period, _ := strconv.ParseInt(os.Getenv(envPeriod), 10, 64)
	limit, _ := strconv.Atoi(os.Getenv(envLimit))
	busy, _ := strconv.Atoi(os.Getenv(envBusyMS))
	key := os.Getenv(envKey)
	st, err := Open(os.Getenv(envDB), period, limit, busy)
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	if os.Getenv(envMode) == "leak" {
		win, ok, err := st.Reserve(ctx, key)
		if err != nil || !ok {
			t.Fatalf("leak reserve = (%v,%v)", ok, err)
		}
		if err := os.WriteFile(os.Getenv(envSentinel), []byte(strconv.FormatInt(win, 10)), 0o644); err != nil {
			t.Fatalf("write sentinel: %v", err)
		}
		select {} // 卡在"预留成功、认领未完成"之间,等父进程 kill -9
	}

	// consume 模式。
	logF, err := os.Create(os.Getenv(envLog))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logF.Close()
	dur, _ := time.ParseDuration(os.Getenv(envDur))
	line := func(format string, a ...any) {
		fmt.Fprintf(logF, format+"\n", a...)
	}
	line("begin %d", time.Now().UnixMilli())
	for until := time.Now().Add(dur); time.Now().Before(until); {
		win, ok, err := st.Reserve(ctx, key)
		switch {
		case err != nil: // 介质不可达:fail-closed,零放行,退避重试
			line("err %d", time.Now().UnixMilli())
			time.Sleep(50 * time.Millisecond)
		case !ok: // 耗尽:不是错误,小睡等下个窗口
			time.Sleep(5 * time.Millisecond)
		default: // 预留成功 = 认领放行 = handler 启动(原型里队列永远非空)
			line("start %d %d", win, time.Now().UnixMilli())
			time.Sleep(2 * time.Millisecond)
		}
	}
	line("end %d", time.Now().UnixMilli())
}

// spawnHelper 拉起一个子进程,返回 cmd(已 Start)。
func spawnHelper(t *testing.T, env map[string]string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestQuotaProcHelper$", "-test.v")
	cmd.Env = append(os.Environ(), envHelper+"=1")
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out := &strings.Builder{}
	cmd.Stdout, cmd.Stderr = out, out
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		if t.Failed() {
			t.Logf("子进程输出:\n%s", out.String())
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	return cmd
}

// startRec 一条"handler 启动"记录。
type startRec struct {
	win int64 // 预留语句返回的窗口起点(介质钟)
	ms  int64 // 子进程本地毫秒时间(只用于 fail-closed 的时段断言)
}

// parseLog 解析子进程日志。
func parseLog(t *testing.T, path string) (starts []startRec, errsMS []int64, beginMS, endMS int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log %s: %v", path, err)
	}
	for ln := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		f := strings.Fields(ln)
		switch {
		case len(f) == 3 && f[0] == "start":
			win, _ := strconv.ParseInt(f[1], 10, 64)
			ms, _ := strconv.ParseInt(f[2], 10, 64)
			starts = append(starts, startRec{win: win, ms: ms})
		case len(f) == 2 && f[0] == "err":
			ms, _ := strconv.ParseInt(f[1], 10, 64)
			errsMS = append(errsMS, ms)
		case len(f) == 2 && f[0] == "begin":
			beginMS, _ = strconv.ParseInt(f[1], 10, 64)
		case len(f) == 2 && f[0] == "end":
			endMS, _ = strconv.ParseInt(f[1], 10, 64)
		}
	}
	return starts, errsMS, beginMS, endMS
}

// baseEnv 场景公共环境变量。
func baseEnv(db string, period int64, limit int) map[string]string {
	return map[string]string{
		envDB:     db,
		envKey:    "gw",
		envPeriod: strconv.FormatInt(period, 10),
		envLimit:  strconv.Itoa(limit),
		envBusyMS: "5000",
	}
}

// TestTwoProcsHardLimit 断言①+②:双进程争同一 quota key,
// 每窗口合计 ≤ N;被完整覆盖的窗口恰好打满 N(窗口切换额度恢复)。
func TestTwoProcsHardLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("多进程原型,-short 跳过")
	}
	const period, limit = 1, 5
	dir := t.TempDir()
	db := filepath.Join(dir, "quota.db")
	st, err := Open(db, period, limit, 5000) // 父进程先建好表
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	env := baseEnv(db, period, limit)
	env[envMode] = "consume"
	env[envDur] = "5s"
	logs := []string{filepath.Join(dir, "w1.log"), filepath.Join(dir, "w2.log")}
	var cmds []*exec.Cmd
	for _, lg := range logs {
		e := map[string]string{}
		maps.Copy(e, env)
		e[envLog] = lg
		cmds = append(cmds, spawnHelper(t, e))
	}
	for _, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Fatalf("helper 退出异常: %v", err)
		}
	}

	// 合并两份日志,按窗口计数;同时求两进程都在跑的公共时段。
	perWin := map[int64]int{}
	commonBegin, commonEnd := int64(0), int64(1<<62)
	for _, lg := range logs {
		starts, _, beginMS, endMS := parseLog(t, lg)
		for _, s := range starts {
			perWin[s.win]++
		}
		commonBegin = max(commonBegin, beginMS)
		commonEnd = min(commonEnd, endMS)
	}

	// 断言①:任何窗口(含边界前后)合计都不超 N。
	for win, n := range perWin {
		if n > limit {
			t.Errorf("窗口 %d 放行 %d 次,超过硬上限 %d", win, n, limit)
		}
	}
	// 断言②:被公共时段完整覆盖的窗口恰好打满 N(额度随窗口恢复,且确实放满)。
	interior := 0
	for win, n := range perWin {
		winStartMS, winEndMS := win*1000, (win+period)*1000
		if winStartMS >= commonBegin && winEndMS <= commonEnd {
			interior++
			if n != limit {
				t.Errorf("完整窗口 %d 放行 %d 次,want 恰好 %d", win, n, limit)
			}
		}
	}
	if interior < 2 {
		t.Fatalf("完整窗口只有 %d 个,样本不足(需 ≥2)", interior)
	}
	t.Logf("窗口分布(%d 个完整窗口): %v", interior, perWin)
}

// TestKillNineLeakConservative 断言③:leak 进程在"预留成功、认领未完成"
// 之间被 kill -9,该窗口总放行恰好 N-1——泄漏只朝保守方向。
func TestKillNineLeakConservative(t *testing.T) {
	if testing.Short() {
		t.Skip("多进程原型,-short 跳过")
	}
	const period, limit = 5, 5
	dir := t.TempDir()
	db := filepath.Join(dir, "quota.db")
	st, err := Open(db, period, limit, 5000)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// 对齐到新窗口开头再动手,保证整个场景落在同一个窗口内。
	nowSec := time.Now().Unix()
	next := (nowSec/period + 1) * period
	time.Sleep(time.Until(time.Unix(next, 0).Add(200 * time.Millisecond)))

	env := baseEnv(db, period, limit)
	env[envMode] = "leak"
	sentinel := filepath.Join(dir, "leaked")
	env[envSentinel] = sentinel
	leak := spawnHelper(t, env)

	// 等哨兵 = 预留已成功;然后 kill -9,这份预留没人退还。
	var leakWin int64
	deadline := time.Now().Add(5 * time.Second)
	for {
		if data, err := os.ReadFile(sentinel); err == nil {
			leakWin, _ = strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等 leak 哨兵超时")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := leak.Process.Kill(); err != nil {
		t.Fatalf("kill -9: %v", err)
	}
	_ = leak.Wait()

	// 消费进程在同一窗口里把剩余额度吃干净。
	env2 := baseEnv(db, period, limit)
	env2[envMode] = "consume"
	env2[envDur] = "1500ms"
	lg := filepath.Join(dir, "consume.log")
	env2[envLog] = lg
	c := spawnHelper(t, env2)
	if err := c.Wait(); err != nil {
		t.Fatalf("consume 退出异常: %v", err)
	}

	starts, _, _, _ := parseLog(t, lg)
	inWin := 0
	for _, s := range starts {
		if s.win == leakWin {
			inWin++
		}
	}
	// 恰好 N-1:泄漏那份额度视同已消耗(只少不多),剩余 N-1 份全被吃掉。
	if inWin != limit-1 {
		t.Fatalf("泄漏窗口 %d 放行 %d 次,want 恰好 %d(N-1)", leakWin, inWin, limit-1)
	}
}

// TestMediumDownFailClosed 断言④:介质不可达(父进程长持写锁模拟)期间
// 零放行且子进程确实观测到故障;锁释放后放行自动恢复。
func TestMediumDownFailClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("多进程原型,-short 跳过")
	}
	const period, limit = 1, 1 << 20 // 额度放到管不着,只让"介质故障"起作用
	dir := t.TempDir()
	db := filepath.Join(dir, "quota.db")
	st, err := Open(db, period, limit, 5000)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	env := baseEnv(db, period, limit)
	env[envMode] = "consume"
	env[envDur] = "3500ms"
	env[envBusyMS] = "100" // 子进程等锁 100ms 就当介质不可达
	lg := filepath.Join(dir, "consume.log")
	env[envLog] = lg
	c := spawnHelper(t, env)

	// 让子进程先正常放行一阵,再长持写锁 1.2s 模拟介质不可达。
	time.Sleep(1 * time.Second)
	ctx := context.Background()
	conn, err := st.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("begin immediate: %v", err)
	}
	t1 := time.Now().UnixMilli()
	time.Sleep(1200 * time.Millisecond)
	t2 := time.Now().UnixMilli()
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}
	conn.Close()
	if err := c.Wait(); err != nil {
		t.Fatalf("consume 退出异常: %v", err)
	}

	starts, errsMS, _, _ := parseLog(t, lg)
	// 故障期(留 300ms 余量吞掉锁前已在途的预留)零放行。
	before, during, afterN := 0, 0, 0
	for _, s := range starts {
		switch {
		case s.ms < t1:
			before++
		case s.ms >= t1+300 && s.ms <= t2:
			during++
		case s.ms > t2+100:
			afterN++
		}
	}
	if before == 0 {
		t.Fatal("故障前没有任何放行,场景没跑起来")
	}
	if during > 0 {
		t.Errorf("介质不可达期间放行了 %d 次,want 0(fail-closed)", during)
	}
	if afterN == 0 {
		t.Error("介质恢复后没有放行,认领没续上")
	}
	// 子进程必须真的观测到了介质故障(err 行落在故障期内)。
	sawErr := false
	for _, ms := range errsMS {
		if ms >= t1 && ms <= t2 {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Error("子进程没观测到介质故障,断言④没被真正触发")
	}
	t.Logf("故障前放行 %d 次,故障中 0 次,恢复后 %d 次,err 记录 %d 条", before, afterN, len(errsMS))
}
