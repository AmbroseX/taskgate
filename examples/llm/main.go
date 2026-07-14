// examples/llm 三级 LLM 流水线示例:检索(retrieve)→ 生成(generate)→ 打分(score)。
//
// 展示的能力:
//   - DependsOn 串联流水线,下游 handler 用 Get 读上游的 Result;
//   - 两条队列不同限流配置(cpu 队列大并发,llm 队列限并发+限速),Routes 按类型路由;
//   - Wait 等结果、Overview 看全局水位、Shutdown 优雅收尾。
//
// 直接跑:go run ./examples/llm(几秒内退出)。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/memorybroker"
)

func main() {
	// 1. 建 Gate:内存后端 + 两条队列。
	//    cpu 队列:本地轻活(检索、打分),4 并发不限速;
	//    llm 队列:调大模型的重活,2 并发、每秒最多放行 3 个,保护网关配额。
	g, err := taskgate.New(taskgate.Config{
		Broker: memorybroker.New(),
		Queues: map[string]taskgate.QueueConfig{
			"cpu": {Workers: 4},
			"llm": {Workers: 2, RPS: 3},
		},
		// Routes:任务类型 → 队列。retrieve/score 是本地活走 cpu,generate 走 llm。
		Routes: map[string]string{
			"retrieve": "cpu",
			"generate": "llm",
			"score":    "cpu",
		},
	})
	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	// 2. 注册三级 handler。每级的入参/出参都是 JSON,靠 Result 串起来。

	// 检索:按问题查资料(这里用假数据模拟)。
	g.Handle("retrieve", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
		var in struct {
			Question string `json:"question"`
		}
		if err := json.Unmarshal(t.Payload, &in); err != nil {
			return nil, taskgate.ErrSkipRetry{Err: err} // 入参坏了,重试也没救
		}
		time.Sleep(50 * time.Millisecond) // 模拟查询耗时
		return json.Marshal(map[string]string{
			"question": in.Question,
			"context":  "关于「" + in.Question + "」的检索资料……",
		})
	})

	// 生成:拿检索结果喂给大模型(模拟)。上游任务 ID 就在 t.DependsOn[0]。
	g.Handle("generate", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
		parent, err := g.Get(ctx, t.DependsOn[0])
		if err != nil {
			return nil, err
		}
		var in map[string]string
		if err := json.Unmarshal(parent.Result, &in); err != nil {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond) // 模拟大模型生成耗时
		return json.Marshal(map[string]string{
			"question": in["question"],
			"answer":   "基于资料生成的回答:" + in["question"] + " 的答案是……",
		})
	})

	// 打分:给生成的回答打质量分(模拟)。
	g.Handle("score", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
		parent, err := g.Get(ctx, t.DependsOn[0])
		if err != nil {
			return nil, err
		}
		var in map[string]string
		if err := json.Unmarshal(parent.Result, &in); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{
			"question": in["question"],
			"answer":   in["answer"],
			"score":    92,
		})
	})

	// 3. 起消费循环(Run 会阻塞,放到 goroutine 里)。
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(context.Background()) }()

	// 4. 提交三条流水线:每个问题都是 检索 → 生成 → 打分 三级串联。
	ctx := context.Background()
	questions := []string{"什么是任务队列", "为什么要限流", "租约是怎么防丢任务的"}
	var scoreIDs []string
	for _, q := range questions {
		payload, _ := json.Marshal(map[string]string{"question": q})
		rid, err := g.Submit(ctx, "retrieve", payload)
		if err != nil {
			log.Fatalf("提交 retrieve 失败: %v", err)
		}
		gid, err := g.Submit(ctx, "generate", nil, taskgate.DependsOn(rid))
		if err != nil {
			log.Fatalf("提交 generate 失败: %v", err)
		}
		sid, err := g.Submit(ctx, "score", nil, taskgate.DependsOn(gid))
		if err != nil {
			log.Fatalf("提交 score 失败: %v", err)
		}
		scoreIDs = append(scoreIDs, sid)
	}

	// 5. 等最后一级出结果。
	for _, id := range scoreIDs {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := g.Wait(wctx, id)
		cancel()
		if err != nil {
			log.Fatalf("等待打分结果失败: %v", err)
		}
		fmt.Printf("流水线产出: %s\n", result)
	}

	// 6. 打全局概览:Type × Status 的数量矩阵。
	ov, err := g.Overview(ctx)
	if err != nil {
		log.Fatalf("Overview 失败: %v", err)
	}
	fmt.Println("\n全局概览(Type × Status):")
	for typ, byStatus := range ov {
		for status, n := range byStatus {
			fmt.Printf("  %-8s %-10s %d\n", typ, status, n)
		}
	}

	// 7. 优雅停机:等在跑任务善终,Run 随之退出。
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := g.Shutdown(sctx); err != nil {
		log.Fatalf("Shutdown 失败: %v", err)
	}
	if err := <-runDone; err != nil {
		log.Fatalf("Run 返回错误: %v", err)
	}
	fmt.Println("\n已优雅停机,示例结束。")
}
