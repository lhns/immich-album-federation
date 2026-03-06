//> using test.dep org.scalameta::munit::1.0.2

import geny.Bytes

import scala.collection.mutable

final case class RunRecord(
    id: Long,
    pairId: Long,
    dryRun: Boolean,
    startedOrder: Long,
    var status: String,
    var message: Option[String],
)

final case class SyncEventRecord(
    runId: Long,
    pairId: Long,
    eventType: String,
    direction: String,
    assetCount: Int,
    payloadText: String,
)

final case class ObservationRecord(
    runId: Long,
    pairId: Long,
    side: String,
    albumId: String,
    peerAssetId: String,
    checksum: String,
    isTrashed: Boolean,
)

final case class TombstoneRecord(
    pairId: Long,
    originSide: String,
    originPeerAssetId: String,
    checksum: String,
    propagateDeletes: Boolean,
)

final class InMemorySyncRepository extends SyncRepository:
  private var nextRunId: Long = 1L
  private var nextOrder: Long = 1L

  val runs: mutable.LinkedHashMap[Long, RunRecord] = mutable.LinkedHashMap.empty
  val events: mutable.ArrayBuffer[SyncEventRecord] = mutable.ArrayBuffer.empty
  val observations: mutable.ArrayBuffer[ObservationRecord] = mutable.ArrayBuffer.empty
  val tombstones: mutable.LinkedHashMap[(Long, String, String), TombstoneRecord] = mutable.LinkedHashMap.empty

  override def startRun(pairId: Long, dryRun: Boolean): Long =
    val runId = nextRunId
    runs.put(
      runId,
      RunRecord(
        id = runId,
        pairId = pairId,
        dryRun = dryRun,
        startedOrder = nextOrder,
        status = "running",
        message = None,
      )
    )
    nextRunId += 1
    nextOrder += 1
    runId

  override def completeRun(runId: Long, status: String, message: Option[String]): Unit =
    val run = runs.getOrElse(runId, throw new RuntimeException(s"Run $runId not found"))
    run.status = status
    run.message = message

  override def findPreviousSuccessfulRunId(pairId: Long): Option[Long] =
    runs.values
      .filter(run => run.pairId == pairId && run.status == "success")
      .toVector
      .sortBy(_.startedOrder)
      .lastOption
      .map(_.id)

  override def getRunObservations(runId: Long, side: String): Vector[(String, String)] =
    observations
      .filter(observation => observation.runId == runId && observation.side == side)
      .map(observation => (observation.peerAssetId, observation.checksum))
      .toVector

  override def saveTombstones(
      pairId: Long,
      originSide: String,
      missingAssets: Vector[(String, String)],
      propagateDeletes: Boolean,
  ): Unit =
    missingAssets.foreach { (assetId, checksum) =>
      tombstones.put(
        (pairId, originSide, assetId),
        TombstoneRecord(
          pairId = pairId,
          originSide = originSide,
          originPeerAssetId = assetId,
          checksum = checksum,
          propagateDeletes = propagateDeletes,
        )
      )
    }

  override def recordSyncEvent(
      runId: Long,
      pairId: Long,
      eventType: String,
      direction: String,
      assetCount: Int,
      payloadText: String,
  ): Unit =
    events += SyncEventRecord(runId, pairId, eventType, direction, assetCount, payloadText)

  override def recordObservation(
      runId: Long,
      pairId: Long,
      side: String,
      albumId: String,
      asset: AssetResponseDto,
  ): Unit =
    val key = (runId, side, asset.id)
    val idx = observations.indexWhere(o => (o.runId, o.side, o.peerAssetId) == key)
    val row = ObservationRecord(
      runId = runId,
      pairId = pairId,
      side = side,
      albumId = albumId,
      peerAssetId = asset.id,
      checksum = asset.checksum,
      isTrashed = asset.isTrashed,
    )
    if (idx >= 0) {
      observations.update(idx, row)
    } else {
      observations += row
    }

final class FakeImmichApi extends ImmichApi:
  private val albumAssets: mutable.Map[(String, String), Seq[AssetResponseDto]] = mutable.Map.empty
  private val bulkChecks: mutable.Map[String, Seq[BulkCheckResp[AssetResponseDto]]] = mutable.Map.empty
  private val bytesByAssetId: mutable.Map[String, Bytes] = mutable.Map.empty
  private val uploadIds: mutable.Queue[String] = mutable.Queue.empty
  private val albumFailures: mutable.Map[(String, String), Throwable] = mutable.Map.empty

  val untrashCalls: mutable.ArrayBuffer[(String, Seq[String])] = mutable.ArrayBuffer.empty
  val addToAlbumCalls: mutable.ArrayBuffer[(String, Seq[String])] = mutable.ArrayBuffer.empty
  val assetGetCalls: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer.empty
  val uploadCalls: mutable.ArrayBuffer[(String, AssetUploadRequest)] = mutable.ArrayBuffer.empty

  def setAlbumAssets(baseUrl: String, albumId: String, assets: Seq[AssetResponseDto]): Unit =
    albumAssets.put((baseUrl, albumId), assets)

  def setBulkCheck(baseUrl: String, results: Seq[BulkCheckResp[AssetResponseDto]]): Unit =
    bulkChecks.put(baseUrl, results)

  def setAssetBytes(assetId: String, data: Array[Byte]): Unit =
    bytesByAssetId.put(assetId, Bytes(data))

  def enqueueUploadId(id: String): Unit =
    uploadIds.enqueue(id)

  def failAlbumGet(baseUrl: String, albumId: String, error: Throwable): Unit =
    albumFailures.put((baseUrl, albumId), error)

  override def albumGetAssets(album: Album): Seq[AssetResponseDto] =
    albumFailures.get((album.server.baseUrl, album.id)).foreach(throw _)
    albumAssets.getOrElse((album.server.baseUrl, album.id), Seq.empty)

  override def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]] =
    bulkChecks.getOrElse(server.baseUrl, assets.map(asset => BulkCheckResp(asset, None, false)))

  override def untrash(server: ImmichServer, assetIds: Seq[String]): Unit =
    untrashCalls += ((server.baseUrl, assetIds))

  override def albumAddAssets(album: Album, assets: Seq[String]): Unit =
    addToAlbumCalls += ((album.server.baseUrl, assets))

  override def assetGet(server: ImmichServer, assetId: String): Bytes =
    assetGetCalls += ((server.baseUrl, assetId))
    bytesByAssetId.getOrElse(assetId, Bytes(Array.emptyByteArray))

  override def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String =
    uploadCalls += ((server.baseUrl, uploadRequest))
    if (uploadIds.nonEmpty) uploadIds.dequeue()
    else s"uploaded-${uploadCalls.size}"

class SyncSuite extends munit.FunSuite:
  private val pair = AlbumPair(
    id = 1L,
    name = "pair-1",
    leftPeerId = 10L,
    leftAlbumId = "album-left",
    rightPeerId = 11L,
    rightAlbumId = "album-right",
    mode = "left_to_right",
    propagateDeletes = true,
    enabled = true,
  )

  private val leftPeer = SyncPeer(10L, "left", "http://left.local", "LEFT_KEY", enabled = true)
  private val rightPeer = SyncPeer(11L, "right", "http://right.local", "RIGHT_KEY", enabled = true)

  private val safety = SafetyConfig(
    allowedHosts = Set("left.local", "right.local"),
    blockedHosts = Set.empty,
    allowPrivateNetworks = false,
  )

  private def mkAsset(id: String, checksum: String, isTrashed: Boolean = false): AssetResponseDto =
    AssetResponseDto(
      checksum = checksum,
      createdAt = "2024-01-01T00:00:00.000Z",
      deviceAssetId = s"device-$id",
      deviceId = "device-1",
      duplicateId = None,
      duration = "00:00:00.000000",
      exifInfo = None,
      fileCreatedAt = "2024-01-01T00:00:00.000Z",
      fileModifiedAt = "2024-01-01T00:00:00.000Z",
      hasMetadata = true,
      id = id,
      isArchived = false,
      isFavorite = false,
      isOffline = false,
      isTrashed = isTrashed,
      livePhotoVideoId = None,
      localDateTime = "2024-01-01T00:00:00.000Z",
      originalFileName = s"$id.jpg",
      originalMimeType = Some("image/jpeg"),
      originalPath = s"/$id.jpg",
      ownerId = "owner-1",
      updatedAt = "2024-01-01T00:00:00.000Z",
      visibility = "timeline",
    )

  test("detectMissing returns previous assets absent from current") {
    val previous = Vector("a1" -> "c1", "a2" -> "c2")
    val current = Vector("a2" -> "c2", "a3" -> "c3")

    assertEquals(detectMissing(previous, current), Vector("a1" -> "c1"))
  }

  test("executePairSyncWith syncs write-enabled flow with untrash upload and album add") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()

    val source1 = mkAsset("left-1", "chk-1")
    val source2 = mkAsset("left-2", "chk-2")
    val source3 = mkAsset("left-3", "chk-3")

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(source1, source2, source3))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)
    api.setBulkCheck(
      rightPeer.baseUrl,
      Seq(
        BulkCheckResp(source1, Some("target-trashed"), isTrashed = true),
        BulkCheckResp(source2, None, isTrashed = false),
        BulkCheckResp(source3, Some("target-existing"), isTrashed = false),
      )
    )
    api.setAssetBytes(source2.id, Array[Byte](1, 2, 3))
    api.enqueueUploadId("target-uploaded")

    executePairSyncWith(
      pair = pair,
      leftPeer = leftPeer,
      rightPeer = rightPeer,
      safety = safety,
      applyWrites = true,
      repo = repo,
      api = api,
      resolveApiKey = _ => "dummy-key",
    )

    assertEquals(repo.runs(1L).status, "success")
    assertEquals(repo.runs(1L).dryRun, false)

    assertEquals(api.untrashCalls.size, 1)
    assertEquals(api.untrashCalls.head._2, Seq("target-trashed"))

    assertEquals(api.uploadCalls.size, 1)
    assertEquals(api.assetGetCalls.size, 1)
    assertEquals(api.assetGetCalls.head._2, source2.id)

    assertEquals(api.addToAlbumCalls.size, 1)
    assertEquals(api.addToAlbumCalls.head._2.toSet, Set("target-uploaded", "target-existing"))

    val directionEvents = repo.events.filter(_.eventType == "direction_sync")
    assertEquals(directionEvents.size, 1)
    assertEquals(directionEvents.head.direction, "left_to_right")
    assertEquals(directionEvents.head.assetCount, 3)
  }

  test("executePairSyncWith records tombstones and deletion event from previous successful run") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()

    val disappeared = mkAsset("left-old", "chk-old")
    val stillRight = mkAsset("right-still", "chk-r")

    val previousRun = repo.startRun(pair.id, dryRun = true)
    repo.recordObservation(previousRun, pair.id, "left", pair.leftAlbumId, disappeared)
    repo.recordObservation(previousRun, pair.id, "right", pair.rightAlbumId, stillRight)
    repo.completeRun(previousRun, "success", None)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight))

    executePairSyncWith(
      pair = pair,
      leftPeer = leftPeer,
      rightPeer = rightPeer,
      safety = safety,
      applyWrites = true,
      repo = repo,
      api = api,
      resolveApiKey = _ => "dummy-key",
    )

    assert(repo.tombstones.contains((pair.id, "left", disappeared.id)))
    assertEquals(repo.tombstones((pair.id, "left", disappeared.id)).propagateDeletes, true)

    val deletionEvents = repo.events.filter(_.eventType == "deletion_detected")
    assertEquals(deletionEvents.size, 1)
    assertEquals(deletionEvents.head.direction, "left")
    assertEquals(deletionEvents.head.assetCount, 1)
  }

  test("executePairSyncWith marks run as failed when API throws") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()

    api.failAlbumGet(leftPeer.baseUrl, pair.leftAlbumId, RuntimeException("boom"))

    val error = intercept[RuntimeException] {
      executePairSyncWith(
        pair = pair,
        leftPeer = leftPeer,
        rightPeer = rightPeer,
        safety = safety,
        applyWrites = true,
        repo = repo,
        api = api,
        resolveApiKey = _ => "dummy-key",
      )
    }

    assertEquals(error.getMessage, "boom")
    assertEquals(repo.runs(1L).status, "failed")
    assertEquals(repo.runs(1L).message, Some("boom"))
  }
