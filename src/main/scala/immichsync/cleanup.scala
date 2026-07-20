package immichsync

// Opt-in orphan cleanup for the sync user's library (peer config: cleanupOrphans).
//
// Reclaims assets THIS TOOL uploaded (uploaded_asset provenance) that no longer belong
// to any album — e.g. copies left behind when a linked album was deleted or re-grouped.
// Unlinking itself never destroys anything; this pass is the only reclaim path.
//
// Guard stack, every candidate must pass ALL of:
//   1. provenance (tool-created upload)                       — structural scope limit
//   2. older than CleanupConfig.afterDays                     — protects in-flight syncs
//   3. exists, not favorited, not archived, not trashed       — live check per asset
//   4. member of no sync-visible album AND albumsContaining() — checked both ways
//   5. not the motion video of any album-resident live photo
//   6. peer has no quarantined pair                           — frozen state stays frozen
//   7. per-pass cap (0 = unlimited)
//   8. trash only, everything logged to deletion_log ('cleanup_trash')
//
// Safety net beyond the guards: if a still-wanted asset is ever trashed, the next sync
// cycle finds its checksum in the trash and restores it (untrash + album-add).

import com.augustnagro.magnum.*

import java.time.OffsetDateTime
import scala.util.control.NonFatal

case class ProvenanceRow(assetId: String, checksum: String, uploadedAt: OffsetDateTime)

case class CleanupFacts(
  provenance: Vector[ProvenanceRow],
  albumMemberIds: Set[String],
  livePhotoVideoIds: Set[String],
  now: OffsetDateTime,
  afterDays: Int,
  maxPerPass: Int,
)

case class CleanupSelection(candidates: Seq[ProvenanceRow], cappedRemainder: Int)

// Pure pre-selection: age gate + album/live-photo exclusion + cap. The remaining
// per-asset live checks (favorite/archived/trashed/albumsContaining) need API calls
// and happen in the orchestrator.
def selectCleanupCandidates(facts: CleanupFacts): CleanupSelection =
  val cutoff = facts.now.minusDays(facts.afterDays.toLong)
  val eligible = facts.provenance
    .filter(_.uploadedAt.isBefore(cutoff))
    .filterNot(row => facts.albumMemberIds.contains(row.assetId))
    .filterNot(row => facts.livePhotoVideoIds.contains(row.assetId))
    .sortBy(_.uploadedAt) // oldest first: deterministic under a cap
  if (facts.maxPerPass > 0 && eligible.size > facts.maxPerPass)
    CleanupSelection(eligible.take(facts.maxPerPass), eligible.size - facts.maxPerPass)
  else
    CleanupSelection(eligible, 0)

def listUploadedAssets(peerId: Long)(using DbCon): Vector[ProvenanceRow] =
  sql"""
      SELECT asset_id, checksum, created_at
      FROM uploaded_asset
      WHERE peer_id = $peerId
    """.query[(String, String, OffsetDateTime)].run().map(ProvenanceRow.apply.tupled)

def insertCleanupDeletion(peerId: Long, assetId: String, checksum: String)(using DbTx): Unit =
  sql"""
      INSERT INTO deletion_log(run_id, pair_id, peer_id, album_id, asset_id, checksum, action, created_at)
      VALUES (NULL, NULL, $peerId, '', $assetId, $checksum, 'cleanup_trash', now())
    """.update.run()

def runOrphanCleanup(
    db: DbRuntime,
    api: ImmichApi,
    peers: Seq[SyncPeer],
    resolveApiKey: SyncPeer => String,
    applyWrites: Boolean,
    cleanup: CleanupConfig,
): Unit =
  peers.filter(_.cleanupOrphans).foreach { peer =>
    try {
      if (connect(db.xa)(existsQuarantinedPairForPeer(peer.id))) {
        println(s"[cleanup peer=${peer.name}] skipped: peer has a quarantined pair")
      } else {
        val effectiveApi = if (applyWrites) api else DryRunImmichApi(api)
        val server = ImmichServer(peer.baseUrl, resolveApiKey(peer))

        // A failed album scan aborts this peer's cleanup: an unreachable server must
        // never look like "no albums reference anything".
        val albums = effectiveApi.listAlbums(server)
        val members = albums.flatMap(album => effectiveApi.albumGetAssets(Album(server, album.id)))
        val facts = CleanupFacts(
          provenance = connect(db.xa)(listUploadedAssets(peer.id)),
          albumMemberIds = members.map(_.id).toSet,
          livePhotoVideoIds = members.flatMap(_.livePhotoVideoId).toSet,
          now = OffsetDateTime.now(),
          afterDays = cleanup.afterDays,
          maxPerPass = cleanup.maxPerPass,
        )
        val selection = selectCleanupCandidates(facts)

        // Live per-asset verification right before acting.
        val confirmed = selection.candidates.filter { row =>
          try {
            val asset = effectiveApi.assetInfo(server, row.assetId)
            !asset.isFavorite && !asset.isArchived && !asset.isTrashed &&
              effectiveApi.albumsContainingAsset(server, row.assetId).isEmpty
          } catch {
            case NonFatal(_) => false // gone or unreadable: nothing to reclaim
          }
        }

        if (confirmed.nonEmpty) {
          effectiveApi.trashAssets(server, confirmed.map(_.assetId))
          if (applyWrites) {
            transact(db.xa):
              confirmed.foreach(row => insertCleanupDeletion(peer.id, row.assetId, row.checksum))
          }
        }
        val capped = if (selection.cappedRemainder > 0) s" (${selection.cappedRemainder} deferred to the next pass)" else ""
        val verb = if (applyWrites) "trashed" else "would trash"
        println(s"[cleanup peer=${peer.name}] $verb ${confirmed.size} orphaned asset(s)$capped")
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"[cleanup peer=${peer.name}] failed: ${Option(e.getMessage).getOrElse(e.toString)}")
    }
  }
