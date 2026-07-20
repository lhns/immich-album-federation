package immichsync

// Repository and persistence (magnum + Flyway). SQL is kept portable across
// PostgreSQL (production) and H2 in MODE=PostgreSQL (integration tests).

import com.augustnagro.magnum.*
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway

trait SyncRepository:
  def startRun(pairId: Long, dryRun: Boolean): Long
  def completeRun(runId: Long, status: String, message: Option[String]): Unit
  def findPreviousBaselineRunId(pairId: Long): Option[Long]
  def getRunObservations(runId: Long, side: String): Vector[ObservationRow]
  def loadActiveTombstones(pairId: Long): Vector[ActiveTombstone]
  def recordSyncEvent(
      runId: Long,
      pairId: Long,
      eventType: String,
      direction: String,
      assetCount: Int,
      payloadText: String,
  ): Unit
  def recordUploadedAsset(runId: Long, pairId: Long, peerId: Long, assetId: String, checksum: String): Unit
  def uploadedByTool(peerId: Long, assetIds: Seq[String]): Set[String]
  def recordDeletion(
      runId: Long,
      pairId: Long,
      peerId: Long,
      albumId: String,
      assetId: String,
      checksum: String,
      action: String,
  ): Unit
  def markQuarantined(pairId: Long, reason: String, rearmKey: String): Unit
  // applyRun = this was a real (non-dry) run: only then is force_additive cleared and
  // observation retention pruned. Dry runs record observations for audit and nothing else.
  def finalizeRun(
      runId: Long,
      pair: AlbumPair,
      tombstoneWrites: Seq[TombstoneWrite],
      resolutions: Seq[TombstoneResolutionPlan],
      observationsLeft: Seq[ObservationRow],
      observationsRight: Seq[ObservationRow],
      applyRun: Boolean,
      observationKeepRuns: Int = RetentionConfig.Default.observationKeepRuns,
  ): Unit

case class DbSyncRepository(db: DbRuntime) extends SyncRepository:
  override def startRun(pairId: Long, dryRun: Boolean): Long =
    transact(db.xa):
      insertRunStart(pairId, dryRun)

  override def completeRun(runId: Long, status: String, message: Option[String]): Unit =
    transact(db.xa):
      finishRun(runId, status, message)

  override def findPreviousBaselineRunId(pairId: Long): Option[Long] =
    connect(db.xa):
      previousBaselineRunId(pairId)

  override def getRunObservations(runId: Long, side: String): Vector[ObservationRow] =
    connect(db.xa):
      loadRunObservations(runId, side)

  override def loadActiveTombstones(pairId: Long): Vector[ActiveTombstone] =
    connect(db.xa):
      selectActiveTombstones(pairId)

  override def recordSyncEvent(
      runId: Long,
      pairId: Long,
      eventType: String,
      direction: String,
      assetCount: Int,
      payloadText: String,
  ): Unit =
    transact(db.xa):
      insertSyncEvent(runId, pairId, eventType, direction, assetCount, payloadText)

  override def recordUploadedAsset(runId: Long, pairId: Long, peerId: Long, assetId: String, checksum: String): Unit =
    transact(db.xa):
      insertUploadedAsset(runId, pairId, peerId, assetId, checksum)

  override def uploadedByTool(peerId: Long, assetIds: Seq[String]): Set[String] =
    connect(db.xa):
      assetIds.filter(uploadedAssetExists(peerId, _)).toSet

  override def recordDeletion(
      runId: Long,
      pairId: Long,
      peerId: Long,
      albumId: String,
      assetId: String,
      checksum: String,
      action: String,
  ): Unit =
    transact(db.xa):
      insertDeletionLog(runId, pairId, peerId, albumId, assetId, checksum, action)

  override def markQuarantined(pairId: Long, reason: String, rearmKey: String): Unit =
    transact(db.xa):
      updatePairQuarantined(pairId, reason, rearmKey)

  override def finalizeRun(
      runId: Long,
      pair: AlbumPair,
      tombstoneWrites: Seq[TombstoneWrite],
      resolutions: Seq[TombstoneResolutionPlan],
      observationsLeft: Seq[ObservationRow],
      observationsRight: Seq[ObservationRow],
      applyRun: Boolean,
      observationKeepRuns: Int = RetentionConfig.Default.observationKeepRuns,
  ): Unit =
    transact(db.xa):
      tombstoneWrites.foreach(upsertTombstone(pair.id, _, pair.propagateDeletes))
      resolutions.foreach(r => resolveTombstone(pair.id, r.originSide, r.checksum, r.resolution))
      observationsLeft.foreach(insertObservation(runId, pair.id, "left", pair.leftAlbumId, _))
      observationsRight.foreach(insertObservation(runId, pair.id, "right", pair.rightAlbumId, _))
      if (applyRun && pair.forceAdditive) updatePairForceAdditive(pair.id, forceAdditive = false)
      if (applyRun) pruneObservations(pair.id, observationKeepRuns)
      finishRun(runId, "success", None)

def createDbRuntime(url: String, user: String, password: String, maxPool: Int = 4, minIdle: Int = 1, connTimeoutMs: Long = 10000): DbRuntime =
  val ds = HikariDataSource()
  ds.setJdbcUrl(url)
  ds.setUsername(user)
  ds.setPassword(password)
  ds.setMaximumPoolSize(maxPool)
  ds.setMinimumIdle(minIdle)
  ds.setConnectionTimeout(connTimeoutMs)
  DbRuntime(ds, Transactor(ds))

def createDbRuntimeFromEnv(): DbRuntime =
  createDbRuntime(
    url = requiredEnv("IMMICH_SYNC_DB_URL"),
    user = requiredEnv("IMMICH_SYNC_DB_USER"),
    password = requiredEnv("IMMICH_SYNC_DB_PASSWORD"),
    maxPool = envOrDefault("IMMICH_SYNC_DB_MAX_POOL", "4").toInt,
    minIdle = envOrDefault("IMMICH_SYNC_DB_MIN_IDLE", "1").toInt,
    connTimeoutMs = envOrDefault("IMMICH_SYNC_DB_CONN_TIMEOUT_MS", "10000").toLong,
  )

def runMigrations(db: DbRuntime): Unit =
  val locations = envOrDefault("IMMICH_SYNC_MIGRATION_LOCATIONS", "classpath:db/migration")
  Flyway
    .configure()
    .dataSource(db.dataSource)
    .locations(locations)
    .load()
    .migrate()

// NOTE: magnum 1.3.1 cannot splice SQL fragments (an interpolated Frag becomes a bind
// parameter), so the column lists below are repeated literally; the row mapping is the
// single source of truth. Keep the SELECT lists in sync with PeerRow / PairRow.
private[immichsync] type PeerRow = (Long, String, String, Boolean, Option[Int], Option[Double], Boolean)

private[immichsync] def peerFromRow(row: PeerRow): SyncPeer = row match {
  case (id, name, baseUrl, enabled, maxRemovalCount, maxRemovalFraction, cleanupOrphans) =>
    SyncPeer(id, name, baseUrl, enabled, maxRemovalCount, maxRemovalFraction, cleanupOrphans)
}

def loadEnabledPeers()(using DbCon): Vector[SyncPeer] =
  sql"""
      SELECT id, name, base_url, enabled, max_removal_count, max_removal_fraction, cleanup_orphans
      FROM sync_peer
      WHERE enabled = true
    """.query[PeerRow].run().map(peerFromRow)

def loadAllPeers()(using DbCon): Vector[SyncPeer] =
  sql"""
      SELECT id, name, base_url, enabled, max_removal_count, max_removal_fraction, cleanup_orphans
      FROM sync_peer
    """.query[PeerRow].run().map(peerFromRow)

private[immichsync] type PairRow = (Long, String, Long, String, Long, String, String, Boolean, Boolean, Boolean, Boolean, Option[Int], Option[Double], String)

private[immichsync] def pairFromRow(row: PairRow): AlbumPair = row match {
  case (id, name, leftPeerId, leftAlbumId, rightPeerId, rightAlbumId, mode, propagateDeletes, enabled,
        trashOrphanedAssets, forceAdditive, maxRemovalCount, maxRemovalFraction, linkSource) =>
    AlbumPair(
      id, name, leftPeerId, leftAlbumId, rightPeerId, rightAlbumId, mode, propagateDeletes, enabled,
      trashOrphanedAssets, forceAdditive, maxRemovalCount, maxRemovalFraction, linkSource,
    )
}

def loadEnabledPairs(pairFilter: Option[String])(using DbCon): Vector[AlbumPair] =
  val rows = pairFilter match {
    case Some(name) =>
      sql"""
          SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled, trash_orphaned_assets, force_additive, max_removal_count, max_removal_fraction, link_source
          FROM album_pair
          WHERE enabled = true AND quarantined_at IS NULL AND name = $name
        """.query[PairRow].run()
    case None =>
      sql"""
          SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled, trash_orphaned_assets, force_additive, max_removal_count, max_removal_fraction, link_source
          FROM album_pair
          WHERE enabled = true AND quarantined_at IS NULL
        """.query[PairRow].run()
  }
  rows.map(pairFromRow)

// Only successful, non-dry runs establish a baseline: a dry run observes state but has
// converged nothing, so diffing against it could produce removals that were never agreed.
// Ordered by id (strictly insert-ordered), which is deterministic even when two runs
// share a started_at timestamp; retention protection uses the same definition.
def previousBaselineRunId(pairId: Long)(using DbCon): Option[Long] =
  sql"""
      SELECT MAX(id)
      FROM sync_run
      WHERE pair_id = $pairId
      AND status = 'success'
      AND dry_run = false
    """.query[Option[Long]].run().headOption.flatten

def loadRunObservations(runId: Long, side: String)(using DbCon): Vector[ObservationRow] =
  sql"""
      SELECT peer_asset_id, checksum, is_trashed
      FROM asset_observation
      WHERE run_id = $runId
      AND side = $side
    """.query[(String, String, Boolean)].run().map(ObservationRow.apply.tupled)

def selectActiveTombstones(pairId: Long)(using DbCon): Vector[ActiveTombstone] =
  sql"""
      SELECT origin_side, origin_peer_asset_id, checksum
      FROM tombstone
      WHERE pair_id = $pairId
      AND resolved_at IS NULL
    """.query[(String, String, String)].run().map(ActiveTombstone.apply.tupled)

def insertRunStart(pairId: Long, dryRun: Boolean)(using DbTx): Long =
  // INSERT + SELECT MAX instead of RETURNING (not portable to H2). Safe: a pair is
  // only ever run by one writer at a time, inside this transaction.
  sql"""
      INSERT INTO sync_run(pair_id, status, dry_run, started_at)
      VALUES ($pairId, 'running', $dryRun, now())
    """.update.run()
  sql"""
      SELECT MAX(id) FROM sync_run WHERE pair_id = $pairId
    """.query[Long].run().head

def finishRun(runId: Long, status: String, message: Option[String])(using DbTx): Unit =
  sql"""
      UPDATE sync_run
      SET status = $status,
          message = $message,
          finished_at = now()
      WHERE id = $runId
    """.update.run()

def insertSyncEvent(
    runId: Long,
    pairId: Long,
    eventType: String,
    direction: String,
    assetCount: Int,
    payloadText: String,
)(using DbTx): Unit =
  sql"""
      INSERT INTO sync_event(run_id, pair_id, event_type, direction, asset_count, payload_text, created_at)
      VALUES ($runId, $pairId, $eventType, $direction, $assetCount, $payloadText, now())
    """.update.run()

def insertObservation(
    runId: Long,
    pairId: Long,
    side: String,
    albumId: String,
    observation: ObservationRow,
)(using DbTx): Unit =
  sql"""
      INSERT INTO asset_observation(
        run_id, pair_id, side, album_id, peer_asset_id, checksum, is_trashed, seen_at
      )
      VALUES ($runId, $pairId, $side, $albumId, ${observation.assetId}, ${observation.checksum}, ${observation.isTrashed}, now())
    """.update.run()

def upsertTombstone(pairId: Long, write: TombstoneWrite, propagateDeletes: Boolean)(using DbTx): Unit =
  // resolved_at is computed in Scala and bound as a nullable timestamp; update-then-insert
  // instead of ON CONFLICT DO UPDATE keeps the statement portable to H2 (PG mode).
  val resolvedAt: Option[java.time.OffsetDateTime] =
    write.resolution.map(_ => java.time.OffsetDateTime.now())
  val updated =
    sql"""
        UPDATE tombstone
        SET checksum = ${write.checksum},
            last_seen_at = now(),
            propagate_deletes = $propagateDeletes,
            resolution = ${write.resolution},
            resolved_at = $resolvedAt
        WHERE pair_id = $pairId
        AND origin_side = ${write.originSide}
        AND origin_peer_asset_id = ${write.assetId}
      """.update.run()
  if (updated == 0) {
    sql"""
        INSERT INTO tombstone(
          pair_id,
          origin_side,
          origin_peer_asset_id,
          checksum,
          first_seen_at,
          last_seen_at,
          propagate_deletes,
          resolution,
          resolved_at
        )
        VALUES (
          $pairId, ${write.originSide}, ${write.assetId}, ${write.checksum}, now(), now(), $propagateDeletes,
          ${write.resolution}, $resolvedAt
        )
      """.update.run()
  }

def resolveTombstone(pairId: Long, originSide: String, checksum: String, resolution: String)(using DbTx): Unit =
  sql"""
      UPDATE tombstone
      SET resolution = $resolution,
          resolved_at = now()
      WHERE pair_id = $pairId
      AND origin_side = $originSide
      AND checksum = $checksum
      AND resolved_at IS NULL
    """.update.run()

def insertUploadedAsset(runId: Long, pairId: Long, peerId: Long, assetId: String, checksum: String)(using DbTx): Unit =
  sql"""
      INSERT INTO uploaded_asset(run_id, pair_id, peer_id, asset_id, checksum, created_at)
      VALUES ($runId, $pairId, $peerId, $assetId, $checksum, now())
      ON CONFLICT DO NOTHING
    """.update.run()

def uploadedAssetExists(peerId: Long, assetId: String)(using DbCon): Boolean =
  sql"""
      SELECT EXISTS(
        SELECT 1 FROM uploaded_asset WHERE peer_id = $peerId AND asset_id = $assetId
      )
    """.query[Boolean].run().head

def insertDeletionLog(
    runId: Long,
    pairId: Long,
    peerId: Long,
    albumId: String,
    assetId: String,
    checksum: String,
    action: String,
)(using DbTx): Unit =
  sql"""
      INSERT INTO deletion_log(run_id, pair_id, peer_id, album_id, asset_id, checksum, action, created_at)
      VALUES ($runId, $pairId, $peerId, $albumId, $assetId, $checksum, $action, now())
    """.update.run()

def updatePairQuarantined(pairId: Long, reason: String, rearmKey: String)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET quarantined_at = now(),
          quarantine_reason = $reason,
          rearm_key = $rearmKey,
          updated_at = now()
      WHERE id = $pairId
    """.update.run()

def updatePairForceAdditive(pairId: Long, forceAdditive: Boolean)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET force_additive = $forceAdditive,
          updated_at = now()
      WHERE id = $pairId
    """.update.run()

private def rearmTombstonesByPairId(pairId: Long)(using DbTx): Int =
  sql"""
      UPDATE tombstone
      SET resolution = 'rearmed',
          resolved_at = now()
      WHERE resolved_at IS NULL
      AND pair_id = $pairId
    """.update.run()

private def rearmPairById(pairId: Long)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET quarantined_at = NULL,
          quarantine_reason = NULL,
          rearm_key = NULL,
          force_additive = TRUE,
          updated_at = now()
      WHERE id = $pairId
    """.update.run()

// CLI re-arm by pair name. Returns (pairs re-armed, tombstones cleared).
def rearmPairByName(name: String)(using DbTx): (Int, Int) =
  sql"SELECT id FROM album_pair WHERE name = $name".query[Long].run().headOption match {
    case None => (0, 0)
    case Some(pairId) =>
      val tombstones = rearmTombstonesByPairId(pairId)
      rearmPairById(pairId)
      (1, tombstones)
  }

// One-shot re-arm by key (IMMICH_SYNC_REARM). Returns the re-armed pair's name, or
// None if the key is unknown or already consumed.
def rearmByKey(key: String)(using DbTx): Option[String] =
  sql"""
      SELECT id, name FROM album_pair WHERE rearm_key = $key AND quarantined_at IS NOT NULL
    """.query[(Long, String)].run().headOption.map { (pairId, name) =>
    rearmTombstonesByPairId(pairId)
    rearmPairById(pairId)
    name
  }

// Crash-artifact repair: a previous process that died mid-run leaves status='running'
// rows behind forever (they are excluded from audit pruning). Not sync state: safe to
// run unconditionally at startup under the single-writer assumption.
def markAbandonedRuns()(using DbTx): Int =
  sql"""
      UPDATE sync_run
      SET status = 'aborted',
          message = 'process terminated before the run finished',
          finished_at = now()
      WHERE status = 'running'
    """.update.run()

def existsQuarantinedPairForPeer(peerId: Long)(using DbCon): Boolean =
  sql"""
      SELECT EXISTS(
        SELECT 1 FROM album_pair
        WHERE quarantined_at IS NOT NULL
        AND (left_peer_id = $peerId OR right_peer_id = $peerId)
      )
    """.query[Boolean].run().head

def selectQuarantinedPairs()(using DbCon): Vector[(String, Option[String], Option[String])] =
  sql"""
      SELECT name, quarantine_reason, rearm_key
      FROM album_pair
      WHERE quarantined_at IS NOT NULL
      ORDER BY name
    """.query[(String, Option[String], Option[String])].run()

// ---------------------------------------------------------------------------
// Sync groups & membership (surrogate keys; annotation token / album UUID are
// attributes, never join keys)
// ---------------------------------------------------------------------------

def upsertSyncGroup(token: String)(using DbTx): Long =
  sql"""
      INSERT INTO sync_group(token, created_at)
      VALUES ($token, now())
      ON CONFLICT DO NOTHING
    """.update.run()
  sql"""
      SELECT id FROM sync_group WHERE token = $token
    """.query[Long].run().head

def upsertSyncAlbum(
    peerId: Long,
    albumId: String,
    groupId: Option[Long],
    deletesOpt: Option[Boolean],
    directionOpt: Option[String],
)(using DbTx): Unit =
  val updated =
    sql"""
        UPDATE sync_album
        SET group_id = $groupId,
            deletes_opt = $deletesOpt,
            direction_opt = $directionOpt,
            last_seen_at = now()
        WHERE peer_id = $peerId AND album_id = $albumId
      """.update.run()
  if (updated == 0) {
    sql"""
        INSERT INTO sync_album(peer_id, album_id, group_id, deletes_opt, direction_opt, last_seen_at)
        VALUES ($peerId, $albumId, $groupId, $deletesOpt, $directionOpt, now())
      """.update.run()
  }

def countSyncAlbums()(using DbCon): Long =
  sql"SELECT COUNT(*) FROM sync_album".query[Long].run().head

def countSyncGroups()(using DbCon): Long =
  sql"SELECT COUNT(*) FROM sync_group".query[Long].run().head

// ---------------------------------------------------------------------------
// Retention. Lifecycle classes:
//   never deleted:  active tombstones, baseline observations, uploaded_asset, deletion_log
//   state (count):  asset_observation beyond the newest K runs per pair
//   audit (time):   old sync_runs (cascades sync_event + leftover observations),
//                   resolved tombstones
// ---------------------------------------------------------------------------

def pruneObservations(pairId: Long, keepRuns: Int)(using DbTx): Int =
  // Keeps the newest K runs per pair AND, unconditionally, the baseline run (newest
  // successful non-dry run): dry or failed runs in between must never be able to push
  // the baseline's observations out of the retention window.
  sql"""
      DELETE FROM asset_observation
      WHERE pair_id = $pairId
      AND run_id NOT IN (
        SELECT id FROM sync_run
        WHERE pair_id = $pairId
        ORDER BY started_at DESC
        LIMIT $keepRuns
      )
      AND run_id NOT IN (
        SELECT MAX(id) FROM sync_run
        WHERE pair_id = $pairId
        AND status = 'success'
        AND dry_run = false
      )
    """.update.run()

def pruneAuditData(retentionDays: Int)(using DbTx): (Int, Int) =
  if (retentionDays <= 0) (0, 0)
  else {
    // Cutoff bound as a plain timestamp for PostgreSQL/H2 portability.
    val cutoff = java.time.OffsetDateTime.now().minusDays(retentionDays.toLong)
    // Old runs go (cascading their events and any leftover observations), except each
    // pair's current baseline run (same definition as previousBaselineRunId: highest-id
    // successful non-dry run) and anything still running.
    val runs =
      sql"""
          DELETE FROM sync_run
          WHERE started_at < $cutoff
          AND status <> 'running'
          AND id NOT IN (
            SELECT MAX(id) FROM sync_run
            WHERE status = 'success' AND dry_run = false
            GROUP BY pair_id
          )
        """.update.run()
    // Resolved tombstones are audit; active ones are correctness state and never touched.
    val tombstones =
      sql"""
          DELETE FROM tombstone
          WHERE resolved_at IS NOT NULL
          AND resolved_at < $cutoff
        """.update.run()
    (runs, tombstones)
  }
