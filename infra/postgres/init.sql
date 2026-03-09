CREATE TABLE IF NOT EXISTS flags (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    txn_count INTEGER NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    risk_score INTEGER NOT NULL,
    txn_ids TEXT[] NOT NULL,
    dedupe_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flags_user_time
  ON flags (user_id, window_start, window_end);

CREATE UNIQUE INDEX IF NOT EXISTS flags_dedupe_key_uniq ON flags (dedupe_key);