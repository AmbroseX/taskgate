-- taskgate sqlite 后端表结构(照 specs/001-m1-core-queue/data-model.md 第 3 节)。
-- 所有时间列存 unix 毫秒(由 Go 层 clock.Now().UnixMilli() 传入,SQL 里不用 CURRENT_TIMESTAMP,
-- 否则测试注入的 fakeclock 就失效了);零值时间统一存 0。
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    queue           TEXT NOT NULL,
    payload         BLOB,
    status          TEXT NOT NULL,
    result          BLOB,
    last_error      TEXT NOT NULL DEFAULT '',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_retry       INTEGER NOT NULL DEFAULT 0,
    lease_lost      INTEGER NOT NULL DEFAULT 0,
    throttled       INTEGER NOT NULL DEFAULT 0,
    run_at          INTEGER NOT NULL,            -- unix 毫秒,下同
    depends_on      TEXT NOT NULL DEFAULT '[]',  -- JSON 数组
    on_parent_fail  TEXT NOT NULL DEFAULT 'fail_fast',
    pending_parents INTEGER NOT NULL DEFAULT 0,
    lease_token     TEXT NOT NULL DEFAULT '',
    lease_until     INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0, -- running 任务的取消标记,Heartbeat 时发现

    created_at      INTEGER NOT NULL,
    started_at      INTEGER NOT NULL DEFAULT 0,
    finished_at     INTEGER NOT NULL DEFAULT 0,

    -- Identity 身份模型(spec 005):业务幂等键 + 重放来源指针,创建后均不可变。
    business_key    TEXT NOT NULL DEFAULT '',
    replay_of       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_claim ON tasks(queue, status, run_at);
CREATE INDEX IF NOT EXISTS idx_status ON tasks(status, lease_until);
-- 不变式 1(链头唯一):同键下 replay_of 为空的行至多一条 → 并发同键入队兜底。
CREATE UNIQUE INDEX IF NOT EXISTS uq_chain_head
    ON tasks(business_key) WHERE business_key <> '' AND replay_of = '';
-- 不变式 2(重放来源唯一):链不分叉 → 并发同目标 Replay 兜底。
CREATE UNIQUE INDEX IF NOT EXISTS uq_replay_of
    ON tasks(replay_of) WHERE replay_of <> '';
-- 按键查询(Filter.BusinessKey / History)。
CREATE INDEX IF NOT EXISTS idx_business_key ON tasks(business_key) WHERE business_key <> '';
-- 周期配额(spec 006):qkey + 窗口起点(unix 秒,对齐 period)→ 已用次数。
-- "检查 + 扣减"由单条 INSERT ... ON CONFLICT 原子完成,窗口时间用 sqlite 自己的钟
-- (共享介质是本机文件,本机钟就是介质的服务端钟)。
CREATE TABLE IF NOT EXISTS quota (
    qkey TEXT    NOT NULL,
    win  INTEGER NOT NULL,
    used INTEGER NOT NULL,
    PRIMARY KEY (qkey, win)
);
CREATE TABLE IF NOT EXISTS task_deps (
    child_id  TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    done      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_parent ON task_deps(parent_id, done);
