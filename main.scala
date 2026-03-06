//> using scala 3.7.3
//> using dep com.lihaoyi::requests:0.9.0
//> using dep com.lihaoyi::ujson:4.3.2
//> using dep com.lihaoyi::upickle:4.3.2
//> using dep com.augustnagro::magnum:1.3.1
//> using dep org.postgresql:postgresql:42.7.5
//> using dep com.zaxxer:HikariCP:7.0.2
//> using dep org.flywaydb:flyway-core:11.3.4
//> using dep org.flywaydb:flyway-database-postgresql:11.3.4

import com.augustnagro.magnum.*
import com.zaxxer.hikari.HikariDataSource
import geny.Bytes
import org.flywaydb.core.Flyway
import upickle.default.{macroRW, ReadWriter as RW}
import upickle.*

import java.net.{InetAddress, URI}
import java.util.Base64

case class ImmichServer(baseUrl: String, apiKey: String) {
  def apiBaseUrl = s"$baseUrl/api"
}

case class Album(server: ImmichServer, id: String)

case class SyncPeer(
  id: Long,
  name: String,
  baseUrl: String,
  apiKeyEnv: String,
  enabled: Boolean,
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
)

case class CliConfig(
  applyWrites: Boolean,
  pairFilter: Option[String],
  extraAllowedHosts: Set[String],
)

case class SafetyConfig(
  allowedHosts: Set[String],
  blockedHosts: Set[String],
  allowPrivateNetworks: Boolean,
)

case class DbRuntime(dataSource: HikariDataSource, xa: Transactor)

case class BulkCheckResp[A](asset: A, assetId: Option[String], isTrashed: Boolean)

trait ImmichApi:
  def albumGetAssets(album: Album): Seq[AssetResponseDto]
  def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]]
  def untrash(server: ImmichServer, assetIds: Seq[String]): Unit
  def albumAddAssets(album: Album, assets: Seq[String]): Unit
  def assetGet(server: ImmichServer, assetId: String): Bytes
  def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String

trait SyncRepository:
  def startRun(pairId: Long, dryRun: Boolean): Long
  def completeRun(runId: Long, status: String, message: Option[String]): Unit
  def findPreviousSuccessfulRunId(pairId: Long): Option[Long]
  def getRunObservations(runId: Long, side: String): Vector[(String, String)]
  def saveTombstones(
      pairId: Long,
      originSide: String,
      missingAssets: Vector[(String, String)],
      propagateDeletes: Boolean,
  ): Unit
  def recordSyncEvent(
      runId: Long,
      pairId: Long,
      eventType: String,
      direction: String,
      assetCount: Int,
      payloadText: String,
  ): Unit
  def recordObservation(
      runId: Long,
      pairId: Long,
      side: String,
      albumId: String,
      asset: AssetResponseDto,
  ): Unit

case class DbSyncRepository(db: DbRuntime) extends SyncRepository:
  override def startRun(pairId: Long, dryRun: Boolean): Long =
    transact(db.xa):
      insertRunStart(pairId, dryRun)

  override def completeRun(runId: Long, status: String, message: Option[String]): Unit =
    transact(db.xa):
      finishRun(runId, status, message)

  override def findPreviousSuccessfulRunId(pairId: Long): Option[Long] =
    connect(db.xa):
      previousSuccessfulRunId(pairId)

  override def getRunObservations(runId: Long, side: String): Vector[(String, String)] =
    connect(db.xa):
      loadRunObservations(runId, side)

  override def saveTombstones(
      pairId: Long,
      originSide: String,
      missingAssets: Vector[(String, String)],
      propagateDeletes: Boolean,
  ): Unit =
    transact(db.xa):
      upsertTombstones(pairId, originSide, missingAssets, propagateDeletes)

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

  override def recordObservation(
      runId: Long,
      pairId: Long,
      side: String,
      albumId: String,
      asset: AssetResponseDto,
  ): Unit =
    transact(db.xa):
      insertObservation(runId, pairId, side, albumId, asset)

object LiveImmichApi extends ImmichApi:
  override def albumGetAssets(album: Album): Seq[AssetResponseDto] =
    liveAlbumGetAssets(album)

  override def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]] =
    liveAssetBulkCheck(server, assets)

  override def untrash(server: ImmichServer, assetIds: Seq[String]): Unit =
    liveUntrash(server, assetIds)

  override def albumAddAssets(album: Album, assets: Seq[String]): Unit =
    liveAlbumAddAssets(album, assets)

  override def assetGet(server: ImmichServer, assetId: String): Bytes =
    liveAssetGet(server, assetId)

  override def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String =
    liveAssetUpload(server, uploadRequest)

given RW[Bytes] = upickle.readwriter[String].bimap[Bytes](e => Base64.getEncoder.encodeToString(e.array), e => Bytes(Base64.getDecoder.decode(e)))

case class AssetUploadRequest( // upload req also appends fileSize but it doesn't seem to be used
                               deviceAssetId: String, // upload req
                               deviceId: String, // upload req
                               duration: Option[String],
                               fileCreatedAt: String, // upload req
                               fileModifiedAt: String, // upload req
                               filename: Option[String],
                               isFavorite: Option[Boolean], // upload req const false
                               livePhotoVideoId: Option[String],
                               metadata: Option[Seq[ujson.Obj]],
                               sidecarData: Option[Bytes], // upload req optional
                               visibility: Option[String],
                               assetData: Bytes, // upload req
                             ) derives RW {
  def toMultiPart: requests.MultiPart = requests.MultiPart(List[Option[requests.MultiItem]](
    Some(assetData.array).map(requests.MultiItem("assetData", _, filename.get)),
    Some(deviceAssetId).map(requests.MultiItem("deviceAssetId", _)),
    Some(deviceId).map(requests.MultiItem("deviceId", _)),
    //duration.map(requests.MultiItem("duration", _)),
    Some(fileCreatedAt).map(requests.MultiItem("fileCreatedAt", _)),
    Some(fileModifiedAt).map(requests.MultiItem("fileModifiedAt", _)),
    filename.map(requests.MultiItem("filename", _)),
    //isFavorite.map(_.toString).map(requests.MultiItem("isFavorite", _)),
    //livePhotoVideoId.map(requests.MultiItem("livePhotoVideoId", _)),
    //metadata.map(upickle.write(_)).map(requests.MultiItem("metadata", _)),
    //sidecarData.map(requests.MultiItem("sidecarData", _)),
    //visibility.map(requests.MultiItem("visibility", _)),
  ).flatten *)
}

object AssetUploadRequest {
  def fromAssetResponseDto(assetResponseDto: AssetResponseDto, assetData: Array[Byte]): AssetUploadRequest = AssetUploadRequest(
    assetData = Bytes(assetData),
    deviceAssetId = assetResponseDto.deviceAssetId,
    deviceId = assetResponseDto.deviceId,
    duration = None, //Some(assetResponseDto.duration),
    fileCreatedAt = assetResponseDto.fileCreatedAt,
    fileModifiedAt = assetResponseDto.fileModifiedAt,
    filename = Some(assetResponseDto.originalFileName),
    isFavorite = Some(assetResponseDto.isFavorite),
    livePhotoVideoId = None, // assetResponseDto.livePhotoVideoId,
    metadata = None,
    sidecarData = None,
    visibility = None //Some(assetResponseDto.visibility)
  )
}

case class AssetResponseDto(
                             checksum: String,
                             createdAt: String,
                             deviceAssetId: String,
                             deviceId: String,
                             duplicateId: Option[String],
                             duration: String,
                             exifInfo: Option[ujson.Obj],
                             fileCreatedAt: String,
                             fileModifiedAt: String,
                             hasMetadata: Boolean,
                             id: String,
                             isArchived: Boolean,
                             isFavorite: Boolean,
                             isOffline: Boolean,
                             isTrashed: Boolean,
                             livePhotoVideoId: Option[String],
                             localDateTime: String,
                             originalFileName: String,
                             originalMimeType: Option[String],
                             originalPath: String,
                             ownerId: String,
                             updatedAt: String,
                             visibility: String,
                           ) derives RW

def requiredEnv(name: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse {
    throw new RuntimeException(s"Missing required environment variable: $name")
  }

def envOrDefault(name: String, defaultValue: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)

def parseBoolEnv(name: String, defaultValue: Boolean): Boolean =
  sys.env.get(name).map(_.trim.toLowerCase).flatMap {
    case "1" | "true" | "yes" | "on"  => Some(true)
    case "0" | "false" | "no" | "off" => Some(false)
    case _                                  => None
  }.getOrElse(defaultValue)

def parseCsvSet(raw: String): Set[String] =
  raw
    .split(',')
    .map(_.trim)
    .filter(_.nonEmpty)
    .map(_.toLowerCase)
    .toSet

def parseArgs(args: Array[String]): CliConfig =
  args.foldLeft(CliConfig(applyWrites = false, pairFilter = None, extraAllowedHosts = Set.empty)) {
    case (acc, "--apply") => acc.copy(applyWrites = true)
    case (acc, arg) if arg.startsWith("--pair=") =>
      acc.copy(pairFilter = Some(arg.stripPrefix("--pair=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--allow-host=") =>
      val host = arg.stripPrefix("--allow-host=").trim.toLowerCase
      if (host.isEmpty) acc else acc.copy(extraAllowedHosts = acc.extraAllowedHosts + host)
    case (_, arg) =>
      throw new RuntimeException(s"Unknown argument: $arg")
  }

def loadSafetyConfig(cli: CliConfig): SafetyConfig =
  val defaultAllowed = Set("localhost", "127.0.0.1", "::1", "host.docker.internal")
  val fromEnvAllowed = parseCsvSet(envOrDefault("IMMICH_SYNC_ALLOWED_HOSTS", ""))
  val fromEnvBlocked = parseCsvSet(envOrDefault("IMMICH_SYNC_BLOCKED_HOSTS", "immich.lhns.de"))
  SafetyConfig(
    allowedHosts = defaultAllowed ++ fromEnvAllowed ++ cli.extraAllowedHosts,
    blockedHosts = fromEnvBlocked,
    allowPrivateNetworks = parseBoolEnv("IMMICH_SYNC_ALLOW_PRIVATE_NETWORKS", defaultValue = true),
  )

def isPrivateAddress(address: InetAddress): Boolean =
  val host = address.getHostAddress.toLowerCase
  address.isAnyLocalAddress ||
  address.isLoopbackAddress ||
  address.isSiteLocalAddress ||
  address.isLinkLocalAddress ||
  host.startsWith("fc") ||
  host.startsWith("fd")

def assertSafeHost(baseUrl: String, safety: SafetyConfig): Unit =
  val host = Option(URI.create(baseUrl).getHost)
    .map(_.trim.toLowerCase)
    .getOrElse(throw new RuntimeException(s"Invalid base URL, missing host: $baseUrl"))
  if (safety.blockedHosts.contains(host)) {
    throw new RuntimeException(s"Blocked host '$host' is not allowed for sync execution")
  }
  if (safety.allowedHosts.contains(host)) {
    ()
  } else {
    val addresses = InetAddress.getAllByName(host).toVector
    val allPrivate = addresses.nonEmpty && addresses.forall(isPrivateAddress)
    if (!(safety.allowPrivateNetworks && allPrivate)) {
      throw new RuntimeException(
        s"Host '$host' is not in allowlist and does not resolve to private/local addresses"
      )
    }
  }

def createDbRuntime(): DbRuntime =
  val ds = HikariDataSource()
  ds.setJdbcUrl(requiredEnv("IMMICH_SYNC_DB_URL"))
  ds.setUsername(requiredEnv("IMMICH_SYNC_DB_USER"))
  ds.setPassword(requiredEnv("IMMICH_SYNC_DB_PASSWORD"))
  ds.setMaximumPoolSize(envOrDefault("IMMICH_SYNC_DB_MAX_POOL", "4").toInt)
  ds.setMinimumIdle(envOrDefault("IMMICH_SYNC_DB_MIN_IDLE", "1").toInt)
  ds.setConnectionTimeout(envOrDefault("IMMICH_SYNC_DB_CONN_TIMEOUT_MS", "10000").toLong)
  DbRuntime(ds, Transactor(ds))

def runMigrations(db: DbRuntime): Unit =
  val locations = envOrDefault("IMMICH_SYNC_MIGRATION_LOCATIONS", "filesystem:./migrations")
  Flyway
    .configure()
    .dataSource(db.dataSource)
    .locations(locations)
    .load()
    .migrate()

def loadEnabledPeers()(using DbCon): Vector[SyncPeer] =
  sql"""
      SELECT id, name, base_url, api_key_env, enabled
      FROM sync_peer
      WHERE enabled = true
    """.query[(Long, String, String, String, Boolean)].run().map {
    case (id, name, baseUrl, apiKeyEnv, enabled) =>
      SyncPeer(id, name, baseUrl, apiKeyEnv, enabled)
  }

def loadEnabledPairs(pairFilter: Option[String])(using DbCon): Vector[AlbumPair] =
  val rows = pairFilter match {
    case Some(name) =>
      sql"""
          SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled
          FROM album_pair
          WHERE enabled = true AND name = $name
        """.query[(Long, String, Long, String, Long, String, String, Boolean, Boolean)].run()
    case None =>
      sql"""
          SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled
          FROM album_pair
          WHERE enabled = true
        """.query[(Long, String, Long, String, Long, String, String, Boolean, Boolean)].run()
  }
  rows.map {
    case (id, name, leftPeerId, leftAlbumId, rightPeerId, rightAlbumId, mode, propagateDeletes, enabled) =>
      AlbumPair(id, name, leftPeerId, leftAlbumId, rightPeerId, rightAlbumId, mode, propagateDeletes, enabled)
  }

def previousSuccessfulRunId(pairId: Long)(using DbCon): Option[Long] =
  sql"""
      SELECT id
      FROM sync_run
      WHERE pair_id = $pairId
      AND status = 'success'
      ORDER BY started_at DESC
      LIMIT 1
    """.query[Long].run().headOption

def loadRunObservations(runId: Long, side: String)(using DbCon): Vector[(String, String)] =
  sql"""
      SELECT peer_asset_id, checksum
      FROM asset_observation
      WHERE run_id = $runId
      AND side = $side
    """.query[(String, String)].run()

def insertRunStart(pairId: Long, dryRun: Boolean)(using DbTx): Long =
  sql"""
      INSERT INTO sync_run(pair_id, status, dry_run, started_at)
      VALUES ($pairId, 'running', $dryRun, now())
      RETURNING id
    """.returning[Long].run().head

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
    asset: AssetResponseDto,
)(using DbTx): Unit =
  sql"""
      INSERT INTO asset_observation(
        run_id, pair_id, side, album_id, peer_asset_id, checksum, is_trashed, seen_at
      )
      VALUES ($runId, $pairId, $side, $albumId, ${asset.id}, ${asset.checksum}, ${asset.isTrashed}, now())
    """.update.run()

def detectMissing(previous: Vector[(String, String)], current: Vector[(String, String)]): Vector[(String, String)] =
  val currentIds = current.map(_._1).toSet
  previous.filterNot((assetId, _) => currentIds.contains(assetId))

def upsertTombstones(
    pairId: Long,
    originSide: String,
    missingAssets: Vector[(String, String)],
    propagateDeletes: Boolean,
)(using DbTx): Unit =
  missingAssets.foreach { (assetId, checksum) =>
    sql"""
        INSERT INTO tombstone(
          pair_id,
          origin_side,
          origin_peer_asset_id,
          checksum,
          first_seen_at,
          last_seen_at,
          propagate_deletes
        )
        VALUES ($pairId, $originSide, $assetId, $checksum, now(), now(), $propagateDeletes)
        ON CONFLICT(pair_id, origin_side, origin_peer_asset_id)
        DO UPDATE SET
          checksum = EXCLUDED.checksum,
          last_seen_at = now(),
          propagate_deletes = EXCLUDED.propagate_deletes
      """.update.run()
  }

def albumGetAssets(album: Album): Seq[AssetResponseDto] =
  val r = requests.get(
    url = s"${album.server.apiBaseUrl}/albums/${album.id}",
    headers = Map(
      "x-api-key" -> album.server.apiKey,
      "Accept" -> "application/json"
    )
  )
  require(r.is2xx, s"http error: ${r.statusCode}")
  val json = ujson.read(r.text())
  json("assets").arr.map(asset => upickle.read[AssetResponseDto](asset)).toSeq

def assetBulkCheck[A](
    server: ImmichServer,
    assets: Seq[A],
)(
    checksum: A => String
): Seq[BulkCheckResp[A]] =
  if (assets.nonEmpty) {
    val assetByChecksum = assets.map(asset => checksum(asset) -> asset).toMap
    val data = ujson.Obj(
      "assets" -> assetByChecksum.map { (assetChecksum, _) =>
        ujson.Obj(
          "id" -> assetChecksum,
          "checksum" -> assetChecksum
        )
      }
    )
    val r = requests.post(
      url = s"${server.apiBaseUrl}/assets/bulk-upload-check",
      headers = Map(
        "x-api-key" -> server.apiKey,
        "Accept" -> "application/json"
      ),
      data = data
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val json = ujson.read(r.text())
    json("results").arr.map { e =>
      val id = e("id").str
      lazy val asset = assetByChecksum(id)
      e("action").str match {
        case "accept" => BulkCheckResp(asset, None, false)
        case "reject" =>
          val assetId = e("assetId").str
          val isTrashed = e("isTrashed").bool
          BulkCheckResp(asset, Some(assetId), isTrashed)
      }
    }.toSeq
  } else Seq.empty

def untrash(server: ImmichServer, assetIds: Seq[String]): Unit =
  if (assetIds.nonEmpty) {
    val r = requests.post(
      url = s"${server.apiBaseUrl}/trash/restore/assets",
      headers = Map(
        "x-api-key" -> server.apiKey,
        "Accept" -> "application/json"
      ),
      data = ujson.Obj(
        "ids" -> assetIds
      )
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val json = ujson.read(r.text())
    val count = json("count").num.toLong
    require(count == assetIds.size, "not all items could be untrashed")
  }

def albumAddAssets(album: Album, assets: Seq[String]): Unit =
  if (assets.nonEmpty) {
    val r = requests.put(
      url = s"${album.server.apiBaseUrl}/albums/assets",
      headers = Map(
        "x-api-key" -> album.server.apiKey,
        "Accept" -> "application/json"
      ),
      data = ujson.Obj(
        "albumIds" -> List(album.id),
        "assetIds" -> assets
      )
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val json = ujson.read(r.text())
    val success = json("success").bool
    require(success, s"failed to add: ${json}")
  }

def assetGet(server: ImmichServer, assetId: String): Bytes =
  val r = requests.get(
    url = s"${server.apiBaseUrl}/assets/$assetId/original",
    headers = Map(
      "x-api-key" -> server.apiKey
    )
  )
  require(r.is2xx, s"http error: ${r.statusCode}")
  r.data

def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String =
  val r = requests.post(
    url = s"${server.apiBaseUrl}/assets",
    headers = Map(
      "x-api-key" -> server.apiKey,
      "Accept" -> "application/json"
    ),
    data = uploadRequest.toMultiPart
  )
  require(r.is2xx, s"http error: ${r.statusCode}")
  val json = ujson.read(r.text())
  json("status").str match {
    case "created" => json("id").str
    case _ => throw new RuntimeException(s"failed to upload: $json")
  }

private def liveAlbumGetAssets(album: Album): Seq[AssetResponseDto] =
  albumGetAssets(album)

private def liveAssetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]] =
  assetBulkCheck(server, assets)(_.checksum)

private def liveUntrash(server: ImmichServer, assetIds: Seq[String]): Unit =
  untrash(server, assetIds)

private def liveAlbumAddAssets(album: Album, assets: Seq[String]): Unit =
  albumAddAssets(album, assets)

private def liveAssetGet(server: ImmichServer, assetId: String): Bytes =
  assetGet(server, assetId)

private def liveAssetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String =
  assetUpload(server, uploadRequest)

def syncOneDirection(
  repo: SyncRepository,
  api: ImmichApi,
    runId: Long,
    pair: AlbumPair,
    direction: String,
    sourceServer: ImmichServer,
    targetServer: ImmichServer,
    sourceAlbum: Album,
    targetAlbum: Album,
    sourceAssets: Seq[AssetResponseDto],
    targetAssets: Seq[AssetResponseDto],
    applyWrites: Boolean,
): Unit =
  val sourceByChecksum = sourceAssets.groupBy(_.checksum)
  val targetChecksums = targetAssets.map(_.checksum).toSet
  val missingChecksums = sourceByChecksum.keySet.diff(targetChecksums)
  val candidates = sourceAssets.filter(a => missingChecksums.contains(a.checksum))
  if (candidates.nonEmpty) {
    val bulkCheckResults = api.assetBulkCheck(targetServer, candidates)
    val (existingResults, nonExistingResults) = bulkCheckResults.partition(_.assetId.isDefined)
    val toUntrash = existingResults.filter(_.isTrashed).flatMap(_.assetId)

    if (toUntrash.nonEmpty && applyWrites) {
      api.untrash(targetServer, toUntrash)
    }

    val uploadedIds =
      if (applyWrites) {
        nonExistingResults.map { dto =>
          val bytes = api.assetGet(sourceServer, dto.asset.id)
          val uploadRequest = AssetUploadRequest.fromAssetResponseDto(dto.asset, bytes.array)
          api.assetUpload(targetServer, uploadRequest)
        }
      } else {
        Vector.empty[String]
      }

    val addToAlbumIds =
      if (applyWrites) {
        uploadedIds ++ existingResults.filterNot(_.isTrashed).flatMap(_.assetId)
      } else {
        Vector.empty[String]
      }

    if (addToAlbumIds.nonEmpty && applyWrites) {
      api.albumAddAssets(targetAlbum, addToAlbumIds)
    }

    val payload = ujson.Obj(
      "candidates" -> candidates.size,
      "existing" -> existingResults.size,
      "to_untrash" -> toUntrash.size,
      "to_upload" -> nonExistingResults.size,
      "uploaded" -> uploadedIds.size,
      "added_to_album" -> addToAlbumIds.size,
      "apply_writes" -> applyWrites,
    ).render()
    repo.recordSyncEvent(
      runId = runId,
      pairId = pair.id,
      eventType = "direction_sync",
      direction = direction,
      assetCount = candidates.size,
      payloadText = payload,
    )
  }

def executePairSync(
    db: DbRuntime,
    pair: AlbumPair,
    leftPeer: SyncPeer,
    rightPeer: SyncPeer,
    safety: SafetyConfig,
    applyWrites: Boolean,
): Unit =
  executePairSyncWith(
    pair = pair,
    leftPeer = leftPeer,
    rightPeer = rightPeer,
    safety = safety,
    applyWrites = applyWrites,
    repo = DbSyncRepository(db),
    api = LiveImmichApi,
    resolveApiKey = requiredEnv,
  )

def executePairSyncWith(
    pair: AlbumPair,
    leftPeer: SyncPeer,
    rightPeer: SyncPeer,
    safety: SafetyConfig,
    applyWrites: Boolean,
    repo: SyncRepository,
    api: ImmichApi,
    resolveApiKey: String => String = requiredEnv,
): Unit =
  assertSafeHost(leftPeer.baseUrl, safety)
  assertSafeHost(rightPeer.baseUrl, safety)

  val leftServer = ImmichServer(leftPeer.baseUrl, resolveApiKey(leftPeer.apiKeyEnv))
  val rightServer = ImmichServer(rightPeer.baseUrl, resolveApiKey(rightPeer.apiKeyEnv))
  val leftAlbum = Album(leftServer, pair.leftAlbumId)
  val rightAlbum = Album(rightServer, pair.rightAlbumId)

  val runId = repo.startRun(pair.id, dryRun = !applyWrites)

  try {
    val leftAssets = api.albumGetAssets(leftAlbum)
    val rightAssets = api.albumGetAssets(rightAlbum)

    val previousRun = repo.findPreviousSuccessfulRunId(pair.id)
    val previousLeft = previousRun.map(repo.getRunObservations(_, "left")).getOrElse(Vector.empty)
    val previousRight = previousRun.map(repo.getRunObservations(_, "right")).getOrElse(Vector.empty)

    val currentLeft = leftAssets.map(a => (a.id, a.checksum)).toVector
    val currentRight = rightAssets.map(a => (a.id, a.checksum)).toVector

    val missingLeft = detectMissing(previousLeft, currentLeft)
    val missingRight = detectMissing(previousRight, currentRight)

    repo.saveTombstones(pair.id, "left", missingLeft, pair.propagateDeletes)
    repo.saveTombstones(pair.id, "right", missingRight, pair.propagateDeletes)

    if (missingLeft.nonEmpty) {
      repo.recordSyncEvent(
        runId = runId,
        pairId = pair.id,
        eventType = "deletion_detected",
        direction = "left",
        assetCount = missingLeft.size,
        payloadText = ujson.Obj("propagate_deletes" -> pair.propagateDeletes).render(),
      )
    }

    if (missingRight.nonEmpty) {
      repo.recordSyncEvent(
        runId = runId,
        pairId = pair.id,
        eventType = "deletion_detected",
        direction = "right",
        assetCount = missingRight.size,
        payloadText = ujson.Obj("propagate_deletes" -> pair.propagateDeletes).render(),
      )
    }

    leftAssets.foreach(repo.recordObservation(runId, pair.id, "left", pair.leftAlbumId, _))
    rightAssets.foreach(repo.recordObservation(runId, pair.id, "right", pair.rightAlbumId, _))

    pair.mode.toLowerCase match {
      case "bidirectional" =>
        syncOneDirection(
          repo = repo,
          api = api,
          runId = runId,
          pair = pair,
          direction = "left_to_right",
          sourceServer = leftServer,
          targetServer = rightServer,
          sourceAlbum = leftAlbum,
          targetAlbum = rightAlbum,
          sourceAssets = leftAssets,
          targetAssets = rightAssets,
          applyWrites = applyWrites,
        )
        syncOneDirection(
          repo = repo,
          api = api,
          runId = runId,
          pair = pair,
          direction = "right_to_left",
          sourceServer = rightServer,
          targetServer = leftServer,
          sourceAlbum = rightAlbum,
          targetAlbum = leftAlbum,
          sourceAssets = rightAssets,
          targetAssets = leftAssets,
          applyWrites = applyWrites,
        )
      case "left_to_right" =>
        syncOneDirection(
          repo = repo,
          api = api,
          runId = runId,
          pair = pair,
          direction = "left_to_right",
          sourceServer = leftServer,
          targetServer = rightServer,
          sourceAlbum = leftAlbum,
          targetAlbum = rightAlbum,
          sourceAssets = leftAssets,
          targetAssets = rightAssets,
          applyWrites = applyWrites,
        )
      case "right_to_left" =>
        syncOneDirection(
          repo = repo,
          api = api,
          runId = runId,
          pair = pair,
          direction = "right_to_left",
          sourceServer = rightServer,
          targetServer = leftServer,
          sourceAlbum = rightAlbum,
          targetAlbum = leftAlbum,
          sourceAssets = rightAssets,
          targetAssets = leftAssets,
          applyWrites = applyWrites,
        )
      case other =>
        throw new RuntimeException(s"Unsupported pair mode '$other' for pair '${pair.name}'")
    }

    repo.completeRun(runId, status = "success", message = None)
  } catch {
    case e: Throwable =>
      repo.completeRun(runId, status = "failed", message = Some(e.getMessage.take(1000)))
      throw e
  }

@main
def main(args: String*): Unit =
  val cli = parseArgs(args.toArray)
  val safety = loadSafetyConfig(cli)

  if (cli.applyWrites && envOrDefault("IMMICH_ENABLE_WRITES", "") != "YES_I_KNOW") {
    throw new RuntimeException(
      "Writes require explicit confirmation: set IMMICH_ENABLE_WRITES=YES_I_KNOW"
    )
  }

  val db = createDbRuntime()
  try {
    runMigrations(db)

    val (peers, pairs) = connect(db.xa):
      (loadEnabledPeers(), loadEnabledPairs(cli.pairFilter))

    if (pairs.isEmpty) {
      println("No enabled album pairs found. Add records to album_pair.")
    } else {
      val peerById = peers.map(peer => peer.id -> peer).toMap
      pairs.foreach { pair =>
        val leftPeer = peerById.getOrElse(
          pair.leftPeerId,
          throw new RuntimeException(s"Pair '${pair.name}' references missing left peer id ${pair.leftPeerId}")
        )
        val rightPeer = peerById.getOrElse(
          pair.rightPeerId,
          throw new RuntimeException(s"Pair '${pair.name}' references missing right peer id ${pair.rightPeerId}")
        )

        try {
          executePairSync(
            db = db,
            pair = pair,
            leftPeer = leftPeer,
            rightPeer = rightPeer,
            safety = safety,
            applyWrites = cli.applyWrites,
          )
          println(s"[pair=${pair.name}] completed")
        } catch {
          case e: Throwable =>
            println(s"[pair=${pair.name}] failed: ${e.getMessage}")
        }
      }
    }
  } finally {
    db.dataSource.close()
  }
