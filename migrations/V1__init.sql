CREATE TABLE IF NOT EXISTS sync_peer (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  base_url TEXT NOT NULL,
  api_key_env TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS album_pair (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  left_peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  left_album_id TEXT NOT NULL,
  right_peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  right_album_id TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'bidirectional',
  propagate_deletes BOOLEAN NOT NULL DEFAULT FALSE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT album_pair_mode_chk CHECK (mode IN ('bidirectional', 'left_to_right', 'right_to_left'))
);

CREATE TABLE IF NOT EXISTS sync_run (
  id BIGSERIAL PRIMARY KEY,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  dry_run BOOLEAN NOT NULL,
  message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS sync_run_pair_started_idx ON sync_run(pair_id, started_at DESC);

CREATE TABLE IF NOT EXISTS asset_observation (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES sync_run(id) ON DELETE CASCADE,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  side TEXT NOT NULL,
  album_id TEXT NOT NULL,
  peer_asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  is_trashed BOOLEAN NOT NULL,
  seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT asset_observation_side_chk CHECK (side IN ('left', 'right')),
  CONSTRAINT asset_observation_unique UNIQUE (run_id, side, peer_asset_id)
);

CREATE INDEX IF NOT EXISTS asset_observation_pair_side_checksum_idx
  ON asset_observation(pair_id, side, checksum);

CREATE TABLE IF NOT EXISTS tombstone (
  id BIGSERIAL PRIMARY KEY,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  origin_side TEXT NOT NULL,
  origin_peer_asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  propagate_deletes BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT tombstone_origin_side_chk CHECK (origin_side IN ('left', 'right')),
  CONSTRAINT tombstone_unique UNIQUE (pair_id, origin_side, origin_peer_asset_id)
);

CREATE INDEX IF NOT EXISTS tombstone_pair_side_idx
  ON tombstone(pair_id, origin_side, resolved_at);

CREATE TABLE IF NOT EXISTS sync_event (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES sync_run(id) ON DELETE CASCADE,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  direction TEXT NOT NULL,
  asset_count INTEGER NOT NULL DEFAULT 0,
  payload_text TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sync_event_run_idx ON sync_event(run_id, created_at);
