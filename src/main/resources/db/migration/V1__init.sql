-- immich-album-federation schema. SQL is kept portable across PostgreSQL (production)
-- and H2 in MODE=PostgreSQL (integration tests).

-- Registered Immich instances. Surrogate id everywhere; base_url is a mutable
-- attribute (hostname changes never orphan sync state), name is the config handle.
CREATE TABLE IF NOT EXISTS sync_peer (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  base_url TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  max_removal_count INTEGER,
  max_removal_fraction DOUBLE PRECISION,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Sync groups: the annotation token ([sync <token>]) is an attribute resolved to a
-- surrogate id, never a join key.
CREATE TABLE IF NOT EXISTS sync_group (
  id BIGSERIAL PRIMARY KEY,
  token TEXT NOT NULL UNIQUE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Group membership: one row per album ever seen by discovery. Identity is the album
-- (peer surrogate id + stable Immich album UUID), not the token. De-annotated or
-- unshared albums keep their row with group_id = NULL.
CREATE TABLE IF NOT EXISTS sync_album (
  id BIGSERIAL PRIMARY KEY,
  peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE CASCADE,
  album_id TEXT NOT NULL,
  group_id BIGINT REFERENCES sync_group(id) ON DELETE SET NULL,
  deletes_opt BOOLEAN,
  direction_opt TEXT,
  last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  CONSTRAINT sync_album_unique UNIQUE (peer_id, album_id)
);

CREATE INDEX IF NOT EXISTS sync_album_group_idx ON sync_album(group_id);

-- Derived execution unit: one row per linked album pair. Removal propagation is the
-- default; the safety rails (additive first run, circuit breaker, trash-only,
-- tombstones) exist precisely so this is safe to leave on.
CREATE TABLE IF NOT EXISTS album_pair (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  left_peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  left_album_id TEXT NOT NULL,
  right_peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  right_album_id TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'bidirectional',
  propagate_deletes BOOLEAN NOT NULL DEFAULT TRUE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  trash_orphaned_assets BOOLEAN NOT NULL DEFAULT TRUE,
  force_additive BOOLEAN NOT NULL DEFAULT TRUE,
  link_source TEXT NOT NULL DEFAULT 'manual',
  quarantined_at TIMESTAMP WITH TIME ZONE,
  quarantine_reason TEXT,
  -- One-shot re-arm token, generated per quarantine incident and printed to the log.
  -- Consumed (cleared) on re-arm; a new trip generates a new key, so a stale
  -- IMMICH_SYNC_REARM value can never re-arm a later incident.
  rearm_key TEXT,
  max_removal_count INTEGER,
  max_removal_fraction DOUBLE PRECISION,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  CONSTRAINT album_pair_mode_chk CHECK (mode IN ('bidirectional', 'left_to_right', 'right_to_left')),
  CONSTRAINT album_pair_link_source_chk CHECK (link_source IN ('manual', 'annotation')),
  CONSTRAINT album_pair_endpoints_unique UNIQUE (left_peer_id, left_album_id, right_peer_id, right_album_id)
);

CREATE TABLE IF NOT EXISTS sync_run (
  id BIGSERIAL PRIMARY KEY,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  dry_run BOOLEAN NOT NULL,
  message TEXT,
  started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  finished_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS sync_run_pair_started_idx ON sync_run(pair_id, started_at DESC);

-- Per-run album membership snapshots. The newest successful apply run per pair is the
-- merge baseline; older rows are pruned (count-based retention), the baseline never.
CREATE TABLE IF NOT EXISTS asset_observation (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES sync_run(id) ON DELETE CASCADE,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  side TEXT NOT NULL,
  album_id TEXT NOT NULL,
  peer_asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  is_trashed BOOLEAN NOT NULL,
  seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  CONSTRAINT asset_observation_side_chk CHECK (side IN ('left', 'right')),
  CONSTRAINT asset_observation_unique UNIQUE (run_id, side, peer_asset_id)
);

CREATE INDEX IF NOT EXISTS asset_observation_pair_side_checksum_idx
  ON asset_observation(pair_id, side, checksum);

-- Tombstone lifecycle:
--   resolved_at IS NULL           -> active: the origin side removed this checksum; it is not
--                                    copied back to the origin side until re-added or re-armed.
--   'propagated'                  -> removal was applied to the other side
--   'converged'                   -> both sides removed it independently
--   'readded'                     -> the origin side got the photo back (user action)
--   'add_wins'                    -> the other side newly added it; the removal was overridden
--   'rearmed'                     -> cleared by a manual re-arm
-- Active tombstones are correctness state and are never deleted; resolved ones are
-- audit and fall under time-based retention.
CREATE TABLE IF NOT EXISTS tombstone (
  id BIGSERIAL PRIMARY KEY,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  origin_side TEXT NOT NULL,
  origin_peer_asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  resolved_at TIMESTAMP WITH TIME ZONE,
  resolution TEXT,
  propagate_deletes BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT tombstone_origin_side_chk CHECK (origin_side IN ('left', 'right')),
  CONSTRAINT tombstone_resolution_chk
    CHECK (resolution IS NULL OR resolution IN ('propagated', 'converged', 'readded', 'add_wins', 'rearmed')),
  CONSTRAINT tombstone_unique UNIQUE (pair_id, origin_side, origin_peer_asset_id)
);

CREATE INDEX IF NOT EXISTS tombstone_pair_side_idx
  ON tombstone(pair_id, origin_side, resolved_at);

-- Append-only event ledger for debugging/audit (time-based retention via sync_run cascade).
CREATE TABLE IF NOT EXISTS sync_event (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES sync_run(id) ON DELETE CASCADE,
  pair_id BIGINT NOT NULL REFERENCES album_pair(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  direction TEXT NOT NULL,
  asset_count INTEGER NOT NULL DEFAULT 0,
  payload_text TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sync_event_run_idx ON sync_event(run_id, created_at);

-- Provenance: assets this tool created on a peer (upload response status = 'created').
-- Uploads that dedup onto an existing user asset (status 'duplicate') are NOT recorded,
-- so user-originated content is never trash-eligible. Never pruned; must survive run
-- pruning (run/pair FKs are SET NULL).
CREATE TABLE IF NOT EXISTS uploaded_asset (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT REFERENCES sync_run(id) ON DELETE SET NULL,
  pair_id BIGINT REFERENCES album_pair(id) ON DELETE SET NULL,
  peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  CONSTRAINT uploaded_asset_unique UNIQUE (peer_id, asset_id)
);

CREATE INDEX IF NOT EXISTS uploaded_asset_peer_checksum_idx
  ON uploaded_asset(peer_id, checksum);

-- Permanent history of every destructive action this tool performed. Never pruned.
CREATE TABLE IF NOT EXISTS deletion_log (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT REFERENCES sync_run(id) ON DELETE SET NULL,
  pair_id BIGINT REFERENCES album_pair(id) ON DELETE SET NULL,
  peer_id BIGINT NOT NULL REFERENCES sync_peer(id) ON DELETE RESTRICT,
  album_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  action TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  CONSTRAINT deletion_log_action_chk CHECK (action IN ('album_remove', 'trash'))
);

CREATE INDEX IF NOT EXISTS deletion_log_pair_idx ON deletion_log(pair_id, created_at);
CREATE INDEX IF NOT EXISTS deletion_log_peer_asset_idx ON deletion_log(peer_id, asset_id);
