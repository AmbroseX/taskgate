//go:build realgw

// realgw_test.go L5 真实网关冒烟档:手动执行,不进 CI(构建标签隔离,常规构建零引入)。
//
// 跑法:
//
//	LLM_GATEWAY_URL=https://网关地址 LLM_GATEWAY_KEY=密钥 \
//	  go test -tags realgw ./e2e/ -run RealGW -v
//
// 可选:LLM_GATEWAY_MODEL 指定模型名(默认 deepseek-r1)。
//
// 已知坑(测试方案第 7 节,踩过的别再踩):
//  1. reasoning 模型(deepseek-r1 等)会先输出思考再输出正文,max_tokens 给小了
//     思考就把额度吃光、content 为空——max_tokens 必须 ≥600;
//  2. 本机挂了 HTTP_PROXY 时,localhost/内网网关会被代理劫持导致连不上,
//     跑之前记得设 NO_PROXY(或 no_proxy)把网关地址排除掉。
//
// 注意:读环境变量的是"测试",不是库本体——taskgate 库自身仍然不读任何 env(宪法允许)。
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/sqlitebroker"
)

// chatRequest OpenAI 兼容的 chat/completions 请求体(只带用得上的字段)。
type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatResponse 只解析要用的部分。
type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// TestRealGWSmoke 真实网关冒烟:10 个抽取任务 {Workers:2, RPS:1},sqlite 后端,
// 全部 completed 即通过;顺带观察真实 429 走 ErrThrottled 的重排行为。
func TestRealGWSmoke(t *testing.T) {
	gwURL := os.Getenv("LLM_GATEWAY_URL")
	gwKey := os.Getenv("LLM_GATEWAY_KEY")
	if gwURL == "" || gwKey == "" {
		t.Skip("缺 LLM_GATEWAY_URL / LLM_GATEWAY_KEY 环境变量,跳过真实网关冒烟")
	}
	model := os.Getenv("LLM_GATEWAY_MODEL")
	if model == "" {
		model = "deepseek-r1"
	}

	b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "realgw.db"))
	if err != nil {
		t.Fatalf("打开 sqlite 后端失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	g, err := taskgate.New(taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			"extract": {Workers: 2, RPS: 1}, // 温柔档:2 并发、每秒 1 个,别把网关打疼
		},
	})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}

	client := &http.Client{Timeout: 120 * time.Second}
	g.Handle("extract", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(task.Payload, &in); err != nil {
			return nil, taskgate.ErrSkipRetry{Err: err}
		}
		body, err := json.Marshal(chatRequest{
			Model: model,
			Messages: []chatMessage{{
				Role:    "user",
				Content: "只输出下面这句话里出现的城市名,不要输出任何别的内容:" + in.Text,
			}},
			// reasoning 模型的思考也吃 max_tokens,给小了 content 为空,必须 ≥600。
			MaxTokens: 600,
		})
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			gwURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+gwKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err // 网络错误走普通重试
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			// 真实 429:优先用网关给的 Retry-After,没有就默认 5s。
			retryAfter := 5 * time.Second
			if s := resp.Header.Get("Retry-After"); s != "" {
				if sec, err := strconv.Atoi(s); err == nil && sec > 0 {
					retryAfter = time.Duration(sec) * time.Second
				}
			}
			t.Logf("任务 %s 撞上真实 429,%v 后重排", task.ID, retryAfter)
			return nil, taskgate.ErrThrottled{RetryAfter: retryAfter}
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("网关返回 HTTP %d: %s", resp.StatusCode, raw)
		}
		var out chatResponse
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("解析响应失败: %w(body=%s)", err, raw)
		}
		if len(out.Choices) == 0 || out.Choices[0].Message.Content == "" {
			// content 为空多半是 max_tokens 被思考吃光(见文件头已知坑 1)。
			return nil, fmt.Errorf("网关返回空 content(检查 max_tokens): %s", raw)
		}
		return json.Marshal(map[string]string{"city": out.Choices[0].Message.Content})
	})

	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = g.Run(runCtx)
	}()
	t.Cleanup(func() { stopRun(); <-runDone })

	cities := []string{"北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆"}
	ids := make([]string, 0, len(cities))
	for i, c := range cities {
		payload, _ := json.Marshal(map[string]string{
			"text": fmt.Sprintf("第 %d 份材料:会议将在%s召开。", i+1, c),
		})
		id, err := g.Submit(context.Background(), "extract", payload)
		if err != nil {
			t.Fatalf("Submit 失败: %v", err)
		}
		ids = append(ids, id)
	}

	// RPS=1 打 10 个任务,加上重试余量,给 10 分钟额度(手动档,不计 CI 时长)。
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for i, id := range ids {
		result, err := g.Wait(wctx, id)
		if err != nil {
			t.Fatalf("任务 %d(%s)未能完成: %v", i, id, err)
		}
		t.Logf("任务 %d 完成: %s", i, result)
	}

	// 复核:10/10 completed。
	for _, id := range ids {
		task, err := g.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get(%s) 失败: %v", id, err)
		}
		if task.Status != taskgate.StatusCompleted {
			t.Fatalf("任务 %s 应为 completed,实际 %s", id, task.Status)
		}
		if task.Throttled > 0 {
			t.Logf("任务 %s 途中被真实限流重排 %d 次(观察项,不判失败)", id, task.Throttled)
		}
	}
}
