package sqlitebroker

// 本文件只在测试编译时存在,把崩溃注入点开放给测试代码
//(包内测试和 sqlitebroker_test 外部测试包都能用)。生产构建零侵入。

// SetTestHookBeforeAckCommit 设置"Ack 事务提交前"的注入 hook,传 nil 恢复默认。
// 崩溃专项测试(kill -9 子进程模式)用它模拟"终态+唤醒写到一半进程没了":
// hook 里 panic 或直接杀进程,事务未提交等于什么都没写,重启后靠 ReapExpired 兜底。
// 注意:非并发安全,只应在跑任务前的单线程阶段设置。
func SetTestHookBeforeAckCommit(fn func()) {
	testHookBeforeAckCommit = fn
}
