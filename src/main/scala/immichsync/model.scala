package immichsync

import com.augustnagro.magnum.Transactor
import com.zaxxer.hikari.HikariDataSource
import upickle.default.ReadWriter as RW

case class ImmichServer(baseUrl: String, apiKey: String) {
  def apiBaseUrl = s"$baseUrl/api"
}

case class Album(server: ImmichServer, id: String)

case class AlbumSummary(id: String, albumName: String, description: String)

// API keys are not part of the peer row: they live only in the config file and are
// resolved in memory per run (never persisted to the database).
case class SyncPeer(
  id: Long,
  name: String,
  baseUrl: String,
  enabled: Boolean,
  maxRemovalCount: Option[Int] = None,
  maxRemovalFraction: Option[Double] = None,
  cleanupOrphans: Boolean = false,
)

case class AlbumPair(
  id: Long,
  name: String,
  leftPeerId: Long,
  leftAlbumId: String,
  rightPeerId: Long,
  rightAlbumId: String,
  mode: String,
  propagateDeletes: Boolean,
  enabled: Boolean,
  trashOrphanedAssets: Boolean = true,
  forceAdditive: Boolean = true,
  maxRemovalCount: Option[Int] = None,
  maxRemovalFraction: Option[Double] = None,
  linkSource: String = "manual",
)

case class CliConfig(
  dryRun: Boolean,
  pairFilter: Option[String],
  rearmPairs: List[String],
  discoverOnly: Boolean,
  configFile: Option[String],
)

case class Thresholds(maxRemovalCount: Int, maxRemovalFraction: Double)

object Thresholds {
  // The fraction check only fires from this many removals up, so tiny albums
  // (where a single removal is a large fraction) do not quarantine on normal use.
  val FractionMinRemovals = 3
  val Default: Thresholds = Thresholds(maxRemovalCount = 25, maxRemovalFraction = 0.3)
}

case class RetentionConfig(observationKeepRuns: Int, auditRetentionDays: Int)

object RetentionConfig {
  val Default: RetentionConfig = RetentionConfig(observationKeepRuns = 5, auditRetentionDays = 90)
}

// Deliberately aggressive defaults (1 day, no cap): a long age gate would relocate
// risk to a moment nobody is watching (enable flag, surprise bulk pass weeks later),
// and a cap would smear the same deletions across cycles, reducing observability.
// Safety lives in the guard stack, not these numbers.
case class CleanupConfig(afterDays: Int, maxPerPass: Int)

object CleanupConfig {
  val Default: CleanupConfig = CleanupConfig(afterDays = 1, maxPerPass = 0) // 0 = unlimited
}

case class DbRuntime(dataSource: HikariDataSource, xa: Transactor)

case class BulkCheckResp(asset: AssetResponseDto, assetId: Option[String], isTrashed: Boolean)

case class UploadResult(id: String, status: String) {
  def created: Boolean = status == "created"
}

// Result of a best-effort album removal: the sync user may lack permission to remove
// assets that the album owner added natively (only owners can remove others' assets).
// removed = actually removed; missing = already gone (not_found, effectively removed
// but no action was performed); skipped = permission or unknown per-id errors.
case class AlbumRemoveResult(
  removed: Seq[String],
  missing: Seq[String] = Seq.empty,
  skipped: Seq[(String, String)] = Seq.empty,
)

case class ObservationRow(assetId: String, checksum: String, isTrashed: Boolean)

case class ActiveTombstone(originSide: String, assetId: String, checksum: String)

case class TombstoneWrite(
  originSide: String,
  assetId: String,
  checksum: String,
  viaTrash: Boolean,
  resolution: Option[String],
)

case class TombstoneResolutionPlan(originSide: String, checksum: String, resolution: String)

case class MergeInput(
  pair: AlbumPair,
  baselineLeft: Vector[ObservationRow],
  baselineRight: Vector[ObservationRow],
  leftAssets: Seq[AssetResponseDto],
  rightAssets: Seq[AssetResponseDto],
  activeTombstones: Vector[ActiveTombstone],
  thresholdsLeft: Thresholds,
  thresholdsRight: Thresholds,
  forceAdditive: Boolean,
)

case class MergePlan(
  copyLeftToRight: Seq[AssetResponseDto],
  copyRightToLeft: Seq[AssetResponseDto],
  removeFromLeft: Seq[AssetResponseDto],
  removeFromRight: Seq[AssetResponseDto],
  tombstoneWrites: Seq[TombstoneWrite],
  resolutions: Seq[TombstoneResolutionPlan],
  quarantineReason: Option[String],
) {
  def isQuarantined: Boolean = quarantineReason.isDefined
}

object MergePlan {
  def quarantined(reason: String): MergePlan =
    MergePlan(Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, Some(reason))
}

// Immich v3 upload shape: deviceAssetId/deviceId were removed from the upload DTO.
// The content checksum travels in the x-immich-checksum header for pre-storage dedup.
case class AssetUploadRequest(
  assetData: Array[Byte],
  filename: String,
  checksum: Option[String],
  fileCreatedAt: String,
  fileModifiedAt: String,
  duration: Option[String] = None,
  isFavorite: Option[Boolean] = None,
  livePhotoVideoId: Option[String] = None,
  visibility: Option[String] = None,
)

object AssetUploadRequest {
  def fromAssetResponseDto(assetResponseDto: AssetResponseDto, assetData: Array[Byte]): AssetUploadRequest = AssetUploadRequest(
    assetData = assetData,
    filename = assetResponseDto.originalFileName,
    checksum = Some(assetResponseDto.checksum),
    fileCreatedAt = assetResponseDto.fileCreatedAt,
    fileModifiedAt = assetResponseDto.fileModifiedAt,
    duration = assetResponseDto.duration,
    isFavorite = Some(assetResponseDto.isFavorite).filter(identity),
    // "timeline" is the default; hidden/archive/locked carry over (e.g. live photo motion parts).
    visibility = assetResponseDto.visibility.filterNot(_ == "timeline"),
  )
}

// Optional/defaulted fields tolerate the v3 DTO cleanup (deviceAssetId/deviceId removed,
// duration nullable) while still parsing older responses.
case class AssetResponseDto(
  checksum: String,
  id: String,
  fileCreatedAt: String,
  fileModifiedAt: String,
  originalFileName: String,
  createdAt: String = "",
  deviceAssetId: Option[String] = None,
  deviceId: Option[String] = None,
  duplicateId: Option[String] = None,
  duration: Option[String] = None,
  exifInfo: Option[ujson.Obj] = None,
  hasMetadata: Boolean = false,
  isArchived: Boolean = false,
  isFavorite: Boolean = false,
  isOffline: Boolean = false,
  isTrashed: Boolean = false,
  livePhotoVideoId: Option[String] = None,
  localDateTime: String = "",
  originalMimeType: Option[String] = None,
  originalPath: String = "",
  ownerId: String = "",
  updatedAt: String = "",
  visibility: Option[String] = None,
) derives RW
