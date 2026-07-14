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
    finished_at     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_claim ON tasks(queue, status, run_at);
CREATE INDEX IF NOT EXISTS idx_status ON tasks(status, lease_until);
CREATE TABLE IF NOT EXISTS task_deps (
    child_id  TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    done      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_parent ON task_deps(parent_id, done);
