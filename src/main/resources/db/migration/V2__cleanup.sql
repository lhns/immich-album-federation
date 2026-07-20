-- Opt-in orphan cleanup for the sync user's library.

ALTER TABLE sync_peer ADD COLUMN cleanup_orphans BOOLEAN NOT NULL DEFAULT FALSE;

-- Cleanup actions get their own audit tag; cleanup rows carry run_id NULL,
-- pair_id NULL and album_id '' (no run/pair/album context).
ALTER TABLE deletion_log DROP CONSTRAINT deletion_log_action_chk;
ALTER TABLE deletion_log
  ADD CONSTRAINT deletion_log_action_chk
  CHECK (action IN ('album_remove', 'trash', 'cleanup_trash'));
