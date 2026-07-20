package immichsync

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
    viaTrash: Boolean,
    var resolution: Option[String],
    var resolved: Boolean,
)

final case class DeletionRecord(
    runId: Long,
    pairId: Long,
    peerId: Long,
    albumId: String,
    assetId: String,
    checksum: String,
    action: String,
)

// Fast in-memory repository. All methods are synchronized uniformly at the impl level
// because the executor uploads in parallel on virtual threads.
class InMemorySyncRepository extends SyncRepository:
  private var nextRunId: Long = 1L
  private var nextOrder: Long = 1L

  val runs: mutable.LinkedHashMap[Long, RunRecord] = mutable.LinkedHashMap.empty
  val events: mutable.ArrayBuffer[SyncEventRecord] = mutable.ArrayBuffer.empty
  val observations: mutable.ArrayBuffer[ObservationRecord] = mutable.ArrayBuffer.empty
  val tombstones: mutable.LinkedHashMap[(Long, String, String), TombstoneRecord] = mutable.LinkedHashMap.empty
  val uploadedAssets: mutable.ArrayBuffer[(Long, String, String)] = mutable.ArrayBuffer.empty // (peerId, assetId, checksum)
  val deletions: mutable.ArrayBuffer[DeletionRecord] = mutable.ArrayBuffer.empty
  val quarantined: mutable.Map[Long, String] = mutable.Map.empty
  val rearmKeys: mutable.Map[Long, String] = mutable.Map.empty
  val forceAdditiveCleared: mutable.Set[Long] = mutable.Set.empty

  def seedTombstone(pairId: Long, originSide: String, assetId: String, checksum: String): Unit = synchronized {
    tombstones.put(
      (pairId, originSide, assetId),
      TombstoneRecord(pairId, originSide, assetId, checksum, viaTrash = false, resolution = None, resolved = false),
    )
  }

  def seedUploadedAsset(peerId: Long, assetId: String, checksum: String): Unit = synchronized {
    uploadedAssets += ((peerId, assetId, checksum))
  }

  override def startRun(pairId: Long, dryRun: Boolean): Long = synchronized {
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
  }

  override def completeRun(runId: Long, status: String, message: Option[String]): Unit = synchronized {
    val run = runs.getOrElse(runId, throw new RuntimeException(s"Run $runId not found"))
    run.status = status
    run.message = message
  }

  override def findPreviousBaselineRunId(pairId: Long): Option[Long] = synchronized {
    runs.values
      .filter(run => run.pairId == pairId && run.status == "success" && !run.dryRun)
      .toVector
      .sortBy(_.startedOrder)
      .lastOption
      .map(_.id)
  }

  override def getRunObservations(runId: Long, side: String): Vector[ObservationRow] = synchronized {
    observations
      .filter(observation => observation.runId == runId && observation.side == side)
      .map(observation => ObservationRow(observation.peerAssetId, observation.checksum, observation.isTrashed))
      .toVector
  }

  override def loadActiveTombstones(pairId: Long): Vector[ActiveTombstone] = synchronized {
    tombstones.values
      .filter(t => t.pairId == pairId && !t.resolved)
      .map(t => ActiveTombstone(t.originSide, t.originPeerAssetId, t.checksum))
      .toVector
  }

  override def recordSyncEvent(
      runId: Long,
      pairId: Long,
      eventType: String,
      direction: String,
      assetCount: Int,
      payloadText: String,
  ): Unit = synchronized {
    events += SyncEventRecord(runId, pairId, eventType, direction, assetCount, payloadText)
  }

  override def recordUploadedAsset(runId: Long, pairId: Long, peerId: Long, assetId: String, checksum: String): Unit = synchronized {
    if (!uploadedAssets.exists(u => u._1 == peerId && u._2 == assetId)) {
      uploadedAssets += ((peerId, assetId, checksum))
    }
  }

  override def uploadedByTool(peerId: Long, assetIds: Seq[String]): Set[String] = synchronized {
    val ids = assetIds.toSet
    uploadedAssets.collect { case (p, assetId, _) if p == peerId && ids.contains(assetId) => assetId }.toSet
  }

  override def recordDeletion(
      runId: Long,
      pairId: Long,
      peerId: Long,
      albumId: String,
      assetId: String,
      checksum: String,
      action: String,
  ): Unit = synchronized {
    deletions += DeletionRecord(runId, pairId, peerId, albumId, assetId, checksum, action)
  }

  override def markQuarantined(pairId: Long, reason: String, rearmKey: String): Unit = synchronized {
    quarantined.put(pairId, reason)
    rearmKeys.put(pairId, rearmKey)
  }

  override def finalizeRun(
      runId: Long,
      pair: AlbumPair,
      tombstoneWrites: Seq[TombstoneWrite],
      resolutions: Seq[TombstoneResolutionPlan],
      observationsLeft: Seq[ObservationRow],
      observationsRight: Seq[ObservationRow],
      applyRun: Boolean,
      observationKeepRuns: Int = RetentionConfig.Default.observationKeepRuns,
  ): Unit = synchronized {
    tombstoneWrites.foreach { write =>
      tombstones.put(
        (pair.id, write.originSide, write.assetId),
        TombstoneRecord(
          pairId = pair.id,
          originSide = write.originSide,
          originPeerAssetId = write.assetId,
          checksum = write.checksum,
          viaTrash = write.viaTrash,
          resolution = write.resolution,
          resolved = write.resolution.isDefined,
        )
      )
    }
    resolutions.foreach { plan =>
      tombstones.values
        .filter(t => t.pairId == pair.id && t.originSide == plan.originSide && t.checksum == plan.checksum && !t.resolved)
        .foreach { t =>
          t.resolution = Some(plan.resolution)
          t.resolved = true
        }
    }
    observationsLeft.foreach { row =>
      observations += ObservationRecord(runId, pair.id, "left", pair.leftAlbumId, row.assetId, row.checksum, row.isTrashed)
    }
    observationsRight.foreach { row =>
      observations += ObservationRecord(runId, pair.id, "right", pair.rightAlbumId, row.assetId, row.checksum, row.isTrashed)
    }
    if (applyRun && pair.forceAdditive) forceAdditiveCleared += pair.id
    // Same retention rule as the DB repository: only apply runs prune, keeping the
    // newest K runs plus, unconditionally, the baseline run.
    if (applyRun) {
      val keepRunIds = runs.values.filter(_.pairId == pair.id).toVector.sortBy(-_.startedOrder).take(observationKeepRuns).map(_.id).toSet ++
        findPreviousBaselineRunId(pair.id).toSet
      observations.filterInPlace(o => o.pairId != pair.id || keepRunIds.contains(o.runId))
    }
    completeRun(runId, "success", None)
  }

// Fake Immich API; methods synchronized uniformly for parallel executor calls.
final class FakeImmichApi extends ImmichApi:
  private val albumAssets: mutable.Map[(String, String), Seq[AssetResponseDto]] = mutable.Map.empty
  private val bulkChecks: mutable.Map[String, Seq[BulkCheckResp]] = mutable.Map.empty
  private val bytesByAssetId: mutable.Map[String, Array[Byte]] = mutable.Map.empty
  private val uploadResults: mutable.Queue[UploadResult] = mutable.Queue.empty
  private val albumFailures: mutable.Map[(String, String), Throwable] = mutable.Map.empty
  private val albumsContaining: mutable.Map[String, Seq[String]] = mutable.Map.empty
  private val assetInfos: mutable.Map[String, AssetResponseDto] = mutable.Map.empty
  private val albumLists: mutable.Map[String, mutable.ArrayBuffer[AlbumSummary]] = mutable.Map.empty
  private val nonRemovable: mutable.Set[String] = mutable.Set.empty

  val untrashCalls: mutable.ArrayBuffer[(String, Seq[String])] = mutable.ArrayBuffer.empty
  val addToAlbumCalls: mutable.ArrayBuffer[(String, Seq[String])] = mutable.ArrayBuffer.empty
  val removeFromAlbumCalls: mutable.ArrayBuffer[(String, String, Seq[String])] = mutable.ArrayBuffer.empty
  val trashCalls: mutable.ArrayBuffer[(String, Seq[String])] = mutable.ArrayBuffer.empty
  val assetGetCalls: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer.empty
  val uploadCalls: mutable.ArrayBuffer[(String, AssetUploadRequest)] = mutable.ArrayBuffer.empty
  val descriptionUpdates: mutable.ArrayBuffer[(String, String, String)] = mutable.ArrayBuffer.empty // (baseUrl, albumId, description)

  def setAlbumAssets(baseUrl: String, albumId: String, assets: Seq[AssetResponseDto]): Unit = synchronized {
    albumAssets.put((baseUrl, albumId), assets)
  }

  def setBulkCheck(baseUrl: String, results: Seq[BulkCheckResp]): Unit = synchronized {
    bulkChecks.put(baseUrl, results)
  }

  def setAssetBytes(assetId: String, data: Array[Byte]): Unit = synchronized {
    bytesByAssetId.put(assetId, data)
  }

  def enqueueUploadResult(result: UploadResult): Unit = synchronized {
    uploadResults.enqueue(result)
  }

  def setAlbumsContaining(assetId: String, albumIds: Seq[String]): Unit = synchronized {
    albumsContaining.put(assetId, albumIds)
  }

  def setAssetInfo(asset: AssetResponseDto): Unit = synchronized {
    assetInfos.put(asset.id, asset)
  }

  def setAlbumList(baseUrl: String, albums: Seq[AlbumSummary]): Unit = synchronized {
    albumLists.put(baseUrl, mutable.ArrayBuffer.from(albums))
  }

  def markNonRemovable(assetId: String): Unit = synchronized {
    nonRemovable += assetId
  }

  def failAlbumGet(baseUrl: String, albumId: String, error: Throwable): Unit = synchronized {
    albumFailures.put((baseUrl, albumId), error)
  }

  override def serverVersion(server: ImmichServer): String = "v3.0.0-test"

  override def listAlbums(server: ImmichServer): Seq[AlbumSummary] = synchronized {
    albumLists.getOrElse(server.baseUrl, mutable.ArrayBuffer.empty).toSeq
  }

  override def updateAlbumDescription(album: Album, description: String): Unit = synchronized {
    descriptionUpdates += ((album.server.baseUrl, album.id, description))
    albumLists.get(album.server.baseUrl).foreach { list =>
      val idx = list.indexWhere(_.id == album.id)
      if (idx >= 0) list.update(idx, list(idx).copy(description = description))
    }
  }

  override def albumGetAssets(album: Album): Seq[AssetResponseDto] = synchronized {
    albumFailures.get((album.server.baseUrl, album.id)).foreach(throw _)
    albumAssets.getOrElse((album.server.baseUrl, album.id), Seq.empty)
  }

  override def assetInfo(server: ImmichServer, assetId: String): AssetResponseDto = synchronized {
    assetInfos.getOrElse(assetId, throw new RuntimeException(s"no asset info for $assetId"))
  }

  override def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp] = synchronized {
    bulkChecks.getOrElse(server.baseUrl, assets.map(asset => BulkCheckResp(asset, None, false)))
  }

  override def untrash(server: ImmichServer, assetIds: Seq[String]): Unit = synchronized {
    untrashCalls += ((server.baseUrl, assetIds))
  }

  override def albumAddAssets(album: Album, assetIds: Seq[String]): Unit = synchronized {
    addToAlbumCalls += ((album.server.baseUrl, assetIds))
  }

  override def albumRemoveAssets(album: Album, assetIds: Seq[String]): AlbumRemoveResult = synchronized {
    removeFromAlbumCalls += ((album.server.baseUrl, album.id, assetIds))
    val (skipped, removed) = assetIds.partition(nonRemovable.contains)
    AlbumRemoveResult(removed = removed, skipped = skipped.map(_ -> "no_permission"))
  }

  override def albumsContainingAsset(server: ImmichServer, assetId: String): Seq[String] = synchronized {
    albumsContaining.getOrElse(assetId, Seq.empty)
  }

  override def trashAssets(server: ImmichServer, assetIds: Seq[String]): Unit = synchronized {
    trashCalls += ((server.baseUrl, assetIds))
  }

  override def assetGet(server: ImmichServer, assetId: String): Array[Byte] = synchronized {
    assetGetCalls += ((server.baseUrl, assetId))
    bytesByAssetId.getOrElse(assetId, Array.emptyByteArray)
  }

  override def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): UploadResult = synchronized {
    uploadCalls += ((server.baseUrl, uploadRequest))
    if (uploadResults.nonEmpty) uploadResults.dequeue()
    else UploadResult(s"uploaded-${uploadCalls.size}", "created")
  }

class SyncSuite extends munit.FunSuite:
  private val basePair = AlbumPair(
    id = 1L,
    name = "pair-1",
    leftPeerId = 10L,
    leftAlbumId = "album-left",
    rightPeerId = 11L,
    rightAlbumId = "album-right",
    mode = "bidirectional",
    propagateDeletes = false,
    enabled = true,
    forceAdditive = false,
  )

  private val leftPeer = SyncPeer(10L, "left", "http://left.local", enabled = true)
  private val rightPeer = SyncPeer(11L, "right", "http://right.local", enabled = true)

  private def mkAsset(
      id: String,
      checksum: String,
      isTrashed: Boolean = false,
      isFavorite: Boolean = false,
      isArchived: Boolean = false,
  ): AssetResponseDto =
    AssetResponseDto(
      checksum = checksum,
      id = id,
      fileCreatedAt = "2024-01-01T00:00:00.000Z",
      fileModifiedAt = "2024-01-01T00:00:00.000Z",
      originalFileName = s"$id.jpg",
      isTrashed = isTrashed,
      isFavorite = isFavorite,
      isArchived = isArchived,
    )

  private def obs(assetId: String, checksum: String, isTrashed: Boolean = false): ObservationRow =
    ObservationRow(assetId, checksum, isTrashed)

  private def mkInput(
      pair: AlbumPair = basePair,
      baselineLeft: Vector[ObservationRow] = Vector.empty,
      baselineRight: Vector[ObservationRow] = Vector.empty,
      leftAssets: Seq[AssetResponseDto] = Seq.empty,
      rightAssets: Seq[AssetResponseDto] = Seq.empty,
      activeTombstones: Vector[ActiveTombstone] = Vector.empty,
      thresholdsLeft: Thresholds = Thresholds.Default,
      thresholdsRight: Thresholds = Thresholds.Default,
      forceAdditive: Boolean = false,
  ): MergeInput =
    MergeInput(pair, baselineLeft, baselineRight, leftAssets, rightAssets, activeTombstones, thresholdsLeft, thresholdsRight, forceAdditive)

  // -------------------------------------------------------------------------
  // computeMergePlan
  // -------------------------------------------------------------------------

  test("planner: initial link is a pure additive union") {
    val plan = computeMergePlan(mkInput(
      leftAssets = Seq(mkAsset("l1", "c1"), mkAsset("l2", "c2")),
      rightAssets = Seq(mkAsset("r3", "c3")),
      forceAdditive = true,
    ))

    assertEquals(plan.copyLeftToRight.map(_.checksum), Seq("c1", "c2"))
    assertEquals(plan.copyRightToLeft.map(_.checksum), Seq("c3"))
    assert(plan.removeFromLeft.isEmpty)
    assert(plan.removeFromRight.isEmpty)
    assert(plan.tombstoneWrites.isEmpty)
    assertEquals(plan.quarantineReason, None)
  }

  test("planner: force_additive ignores stale baseline and tombstones") {
    val plan = computeMergePlan(mkInput(
      pair = basePair.copy(propagateDeletes = true),
      baselineLeft = Vector(obs("l1", "c1"), obs("lX", "cX")),
      baselineRight = Vector(obs("r1", "c1"), obs("rX", "cX")),
      leftAssets = Seq(mkAsset("l1", "c1")),
      rightAssets = Seq(mkAsset("r1", "c1"), mkAsset("rX", "cX")),
      activeTombstones = Vector(ActiveTombstone("left", "lX", "cX")),
      forceAdditive = true,
    ))

    assert(plan.removeFromRight.isEmpty)
    assert(plan.tombstoneWrites.isEmpty)
    // The suppression tombstone is ignored: cX is copied back to left (union).
    assertEquals(plan.copyRightToLeft.map(_.checksum), Seq("cX"))
  }

  test("planner: removal propagates when deletes enabled") {
    val rightX = mkAsset("rX", "cX")
    val plan = computeMergePlan(mkInput(
      pair = basePair.copy(propagateDeletes = true),
      baselineLeft = Vector(obs("l1", "c1"), obs("lX", "cX")),
      baselineRight = Vector(obs("r1", "c1"), obs("rX", "cX")),
      leftAssets = Seq(mkAsset("l1", "c1")),
      rightAssets = Seq(mkAsset("r1", "c1"), rightX),
    ))

    assertEquals(plan.removeFromRight, Seq(rightX))
    assert(plan.removeFromLeft.isEmpty)
    assert(plan.copyRightToLeft.isEmpty)
    val writes = plan.tombstoneWrites
    assertEquals(writes.map(w => (w.originSide, w.checksum, w.resolution)), Seq(("left", "cX", Some("propagated"))))
  }

  test("planner: removal is suppressed, not propagated, when deletes disabled") {
    val plan = computeMergePlan(mkInput(
      pair = basePair.copy(propagateDeletes = false),
      baselineLeft = Vector(obs("lX", "cX")),
      baselineRight = Vector(obs("rX", "cX")),
      leftAssets = Seq.empty,
      rightAssets = Seq(mkAsset("rX", "cX")),
    ))

    assert(plan.removeFromRight.isEmpty)
    // Not copied back either: the removal is respected on the side that made it.
    assert(plan.copyRightToLeft.isEmpty)
    assertEquals(plan.tombstoneWrites.map(w => (w.originSide, w.checksum, w.resolution)), Seq(("left", "cX", None)))
  }

  test("planner: persisted suppression keeps blocking copy-back") {
    val plan = computeMergePlan(mkInput(
      baselineLeft = Vector.empty,
      baselineRight = Vector(obs("rX", "cX")),
      leftAssets = Seq.empty,
      rightAssets = Seq(mkAsset("rX", "cX")),
      activeTombstones = Vector(ActiveTombstone("left", "lX", "cX")),
    ))

    assert(plan.copyRightToLeft.isEmpty)
    assert(plan.resolutions.isEmpty)
    assert(plan.tombstoneWrites.isEmpty)
  }

  test("planner: fresh add on the other side wins over fresh removal") {
    val rightX = mkAsset("rX", "cX")
    val plan = computeMergePlan(mkInput(
      pair = basePair.copy(propagateDeletes = true),
      baselineLeft = Vector(obs("lX", "cX")),
      baselineRight = Vector.empty,
      leftAssets = Seq.empty,
      rightAssets = Seq(rightX),
    ))

    assert(plan.removeFromRight.isEmpty)
    assertEquals(plan.copyRightToLeft, Seq(rightX))
    assertEquals(plan.tombstoneWrites.map(w => (w.originSide, w.checksum, w.resolution)), Seq(("left", "cX", Some("add_wins"))))
  }

  test("planner: fresh add on the other side wins over persisted suppression") {
    val rightX = mkAsset("rX", "cX")
    val plan = computeMergePlan(mkInput(
      baselineLeft = Vector.empty,
      baselineRight = Vector.empty,
      leftAssets = Seq.empty,
      rightAssets = Seq(rightX),
      activeTombstones = Vector(ActiveTombstone("left", "lX", "cX")),
    ))

    assertEquals(plan.copyRightToLeft, Seq(rightX))
    assertEquals(plan.resolutions, Seq(TombstoneResolutionPlan("left", "cX", "add_wins")))
  }

  test("planner: re-add on the origin side resolves the tombstone") {
    val plan = computeMergePlan(mkInput(
      baselineLeft = Vector(obs("lX2", "cX")),
      baselineRight = Vector.empty,
      leftAssets = Seq(mkAsset("lX2", "cX")),
      rightAssets = Seq.empty,
      activeTombstones = Vector(ActiveTombstone("left", "lX", "cX")),
    ))

    assertEquals(plan.resolutions, Seq(TombstoneResolutionPlan("left", "cX", "readded")))
    // Once re-added on left it flows to right again.
    assertEquals(plan.copyLeftToRight.map(_.checksum), Seq("cX"))
  }

  test("planner: removal on both sides converges") {
    val plan = computeMergePlan(mkInput(
      pair = basePair.copy(propagateDeletes = true),
      baselineLeft = Vector(obs("lX", "cX")),
      baselineRight = Vector(obs("rX", "cX")),
      leftAssets = Seq.empty,
      rightAssets = Seq.empty,
    ))

    assert(plan.removeFromLeft.isEmpty)
    assert(plan.removeFromRight.isEmpty)
    assertEquals(
      plan.tombstoneWrites.map(w => (w.originSide, w.checksum, w.resolution)).toSet,
      Set[(String, String, Option[String])](("left", "cX", Some("converged")), ("right", "cX", Some("converged"))),
    )
  }

  test("planner: mass removal by count quarantines and plans nothing") {
    val baseline = (1 to 30).map(i => obs(s"l$i", s"c$i")).toVector
    val plan = computeMergePlan(mkInput(
      baselineLeft = baseline,
      baselineRight = baseline.map(o => obs(s"r-${o.assetId}", o.checksum)),
      leftAssets = Seq.empty,
      rightAssets = baseline.map(o => mkAsset(s"r-${o.assetId}", o.checksum)),
    ))

    assert(plan.isQuarantined)
    assert(plan.copyLeftToRight.isEmpty && plan.copyRightToLeft.isEmpty)
    assert(plan.removeFromLeft.isEmpty && plan.removeFromRight.isEmpty)
    assert(plan.tombstoneWrites.isEmpty)
  }

  test("planner: mass removal by fraction quarantines, small albums do not") {
    val baseline10 = (1 to 10).map(i => obs(s"l$i", s"c$i")).toVector
    val fractionBreach = computeMergePlan(mkInput(
      baselineLeft = baseline10,
      leftAssets = (1 to 6).map(i => mkAsset(s"l$i", s"c$i")),
    ))
    assert(fractionBreach.isQuarantined)

    // 1 of 3 removed is 33% but below the absolute floor: no quarantine.
    val baseline3 = (1 to 3).map(i => obs(s"l$i", s"c$i")).toVector
    val smallAlbum = computeMergePlan(mkInput(
      baselineLeft = baseline3,
      leftAssets = (1 to 2).map(i => mkAsset(s"l$i", s"c$i")),
    ))
    assert(!smallAlbum.isQuarantined)
    assertEquals(smallAlbum.tombstoneWrites.map(_.checksum), Seq("c3"))
  }

  test("planner: circuit-breaker thresholds apply per side") {
    val baseline = (1 to 5).map(i => obs(s"x$i", s"c$i")).toVector
    val strict = Thresholds(maxRemovalCount = 0, maxRemovalFraction = 1.0)
    val lax = Thresholds(maxRemovalCount = 100, maxRemovalFraction = 1.0)

    // One removal on the LEFT with a strict left threshold quarantines...
    val leftBreach = computeMergePlan(mkInput(
      baselineLeft = baseline,
      leftAssets = (1 to 4).map(i => mkAsset(s"x$i", s"c$i")),
      thresholdsLeft = strict,
      thresholdsRight = lax,
    ))
    assert(leftBreach.isQuarantined)

    // ...while the same removal on the RIGHT is judged by the lax right threshold.
    val rightSide = computeMergePlan(mkInput(
      baselineRight = baseline,
      rightAssets = (1 to 4).map(i => mkAsset(s"x$i", s"c$i")),
      thresholdsLeft = strict,
      thresholdsRight = lax,
    ))
    assert(!rightSide.isQuarantined)
  }

  test("planner: one-way mode gates copies and removal propagation") {
    val pair = basePair.copy(mode = "left_to_right", propagateDeletes = true)
    val rightOnly = mkAsset("rO", "cO")
    val rightX = mkAsset("rX", "cX")
    val plan = computeMergePlan(mkInput(
      pair = pair,
      baselineLeft = Vector(obs("lX", "cX"), obs("l1", "c1"), obs("lY", "cY")),
      baselineRight = Vector(obs("rX", "cX"), obs("rY", "cY")),
      leftAssets = Seq(mkAsset("l1", "c1"), mkAsset("lY", "cY")),
      rightAssets = Seq(rightX, rightOnly),
    ))

    // Right-only content never flows left in left_to_right mode.
    assert(plan.copyRightToLeft.isEmpty)
    // Left's removal of cX propagates to right.
    assertEquals(plan.removeFromRight, Seq(rightX))
    // Right's removal of cY does not touch left, and cY is not pushed back to right.
    assert(plan.removeFromLeft.isEmpty)
    assert(!plan.copyLeftToRight.exists(_.checksum == "cY"))
    val rightTombstone = plan.tombstoneWrites.filter(_.originSide == "right")
    assertEquals(rightTombstone.map(w => (w.checksum, w.resolution)), Seq(("cY", None)))
  }

  test("planner: add-wins holds in one-way mode, the receiver's fresh add is never deleted") {
    val pair = basePair.copy(mode = "left_to_right", propagateDeletes = true)
    val freshRight = mkAsset("rX", "cX")
    val plan = computeMergePlan(mkInput(
      pair = pair,
      baselineLeft = Vector(obs("lX", "cX")),
      baselineRight = Vector.empty,
      leftAssets = Seq.empty,
      rightAssets = Seq(freshRight), // freshly added on the right THIS run
    ))

    // Left removed cX, but the right just added it natively: no removal, and (flow
    // right->left being disabled) no copy back either; the tombstone stays active.
    assert(plan.removeFromRight.isEmpty)
    assert(plan.copyRightToLeft.isEmpty)
    assertEquals(plan.tombstoneWrites.map(w => (w.originSide, w.checksum, w.resolution)), Seq(("left", "cX", None)))
  }

  test("planner: trashed asset counts as removal tagged via_trash") {
    val plan = computeMergePlan(mkInput(
      baselineLeft = Vector(obs("lX", "cX")),
      baselineRight = Vector(obs("rX", "cX")),
      leftAssets = Seq(mkAsset("lX", "cX", isTrashed = true)),
      rightAssets = Seq(mkAsset("rX", "cX")),
    ))

    assertEquals(plan.tombstoneWrites.map(w => (w.originSide, w.checksum, w.viaTrash)), Seq(("left", "cX", true)))
  }

  test("planner: duplicate checksums collapse to one copy representative") {
    val plan = computeMergePlan(mkInput(
      leftAssets = Seq(mkAsset("l2", "cDup"), mkAsset("l1", "cDup")),
      rightAssets = Seq.empty,
      forceAdditive = true,
    ))

    assertEquals(plan.copyLeftToRight.map(_.id), Seq("l1"))
  }

  test("planner: unsupported mode throws") {
    intercept[RuntimeException] {
      computeMergePlan(mkInput(pair = basePair.copy(mode = "sideways")))
    }
  }

  // -------------------------------------------------------------------------
  // executePairSyncWith
  // -------------------------------------------------------------------------

  private def runSync(
      repo: SyncRepository,
      api: ImmichApi,
      pair: AlbumPair = basePair,
      applyWrites: Boolean = true,
      retention: RetentionConfig = RetentionConfig.Default,
  ): Unit =
    executePairSyncWith(
      pair = pair,
      leftPeer = leftPeer,
      rightPeer = rightPeer,
      applyWrites = applyWrites,
      repo = repo,
      api = api,
      resolveApiKey = _ => "dummy-key",
      retention = retention,
    )

  private def seedBaseline(
      repo: InMemorySyncRepository,
      pair: AlbumPair,
      left: Seq[AssetResponseDto],
      right: Seq[AssetResponseDto],
  ): Unit =
    val runId = repo.startRun(pair.id, dryRun = false)
    repo.finalizeRun(
      runId, pair,
      tombstoneWrites = Seq.empty,
      resolutions = Seq.empty,
      observationsLeft = left.map(a => ObservationRow(a.id, a.checksum, a.isTrashed)),
      observationsRight = right.map(a => ObservationRow(a.id, a.checksum, a.isTrashed)),
      applyRun = true,
    )

  test("executor: write-enabled add flow untrashes, uploads, adds to album and records provenance") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(mode = "left_to_right", forceAdditive = true)

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
    api.enqueueUploadResult(UploadResult("target-uploaded", "created"))

    runSync(repo, api, pair)

    assertEquals(repo.runs(1L).status, "success")
    assertEquals(repo.runs(1L).dryRun, false)

    assertEquals(api.untrashCalls.size, 1)
    assertEquals(api.untrashCalls.head._2, Seq("target-trashed"))

    assertEquals(api.uploadCalls.size, 1)
    assertEquals(api.assetGetCalls.size, 1)
    assertEquals(api.assetGetCalls.head._2, source2.id)

    // Untrashed assets are re-added to the album as well.
    assertEquals(api.addToAlbumCalls.size, 1)
    assertEquals(api.addToAlbumCalls.head._2.toSet, Set("target-uploaded", "target-existing", "target-trashed"))

    // Provenance recorded only for the freshly created upload, on the target peer.
    assertEquals(repo.uploadedAssets.toList, List((rightPeer.id, "target-uploaded", "chk-2")))

    val directionEvents = repo.events.filter(_.eventType == "direction_sync")
    assertEquals(directionEvents.size, 1)
    assertEquals(directionEvents.head.direction, "left_to_right")
    assertEquals(directionEvents.head.assetCount, 3)
  }

  test("executor: duplicate upload status records no provenance but still adds to album") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(mode = "left_to_right", forceAdditive = true)

    val source = mkAsset("left-1", "chk-1")
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(source))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)
    api.enqueueUploadResult(UploadResult("already-there", "duplicate"))

    runSync(repo, api, pair)

    assertEquals(api.uploadCalls.size, 1)
    assert(repo.uploadedAssets.isEmpty)
    assertEquals(api.addToAlbumCalls.head._2, Seq("already-there"))
  }

  test("executor: live photo uploads the motion video first and links the still to it") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(mode = "left_to_right", forceAdditive = true)

    val still = mkAsset("l-still", "chk-still").copy(livePhotoVideoId = Some("l-video"))
    val video = mkAsset("l-video", "chk-video")

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(still))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)
    api.setAssetInfo(video)
    api.enqueueUploadResult(UploadResult("t-video", "created"))
    api.enqueueUploadResult(UploadResult("t-still", "created"))

    runSync(repo, api, pair)

    assertEquals(api.uploadCalls.size, 2)
    val videoUpload = api.uploadCalls(0)._2
    val stillUpload = api.uploadCalls(1)._2
    assertEquals(videoUpload.filename, "l-video.jpg")
    assertEquals(videoUpload.livePhotoVideoId, None)
    assertEquals(stillUpload.livePhotoVideoId, Some("t-video"))
    // Both transferred assets carry provenance; only the still lands in the album.
    assertEquals(repo.uploadedAssets.map(u => (u._2, u._3)).toSet, Set(("t-video", "chk-video"), ("t-still", "chk-still")))
    assertEquals(api.addToAlbumCalls.head._2, Seq("t-still"))
  }

  test("executor: removal propagates end-to-end with album remove, trash orphan and deletion log") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val keptLeft = mkAsset("l-keep", "chk-keep")
    val keptRight = mkAsset("r-keep", "chk-keep")
    val goneLeft = mkAsset("l-gone", "chk-gone")
    val stillRight = mkAsset("r-gone", "chk-gone")

    seedBaseline(repo, pair, Seq(keptLeft, goneLeft), Seq(keptRight, stillRight))
    repo.seedUploadedAsset(rightPeer.id, stillRight.id, stillRight.checksum)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(keptLeft))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(keptRight, stillRight))

    runSync(repo, api, pair)

    assertEquals(api.removeFromAlbumCalls.toList, List((rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight.id))))
    assertEquals(api.trashCalls.toList, List((rightPeer.baseUrl, Seq(stillRight.id))))
    // chk-gone is not copied back to left.
    assert(api.uploadCalls.isEmpty)
    assert(api.addToAlbumCalls.isEmpty)

    assertEquals(
      repo.deletions.map(d => (d.peerId, d.assetId, d.action)).toList,
      List((rightPeer.id, stillRight.id, "album_remove"), (rightPeer.id, stillRight.id, "trash")),
    )
    val tombstone = repo.tombstones((pair.id, "left", goneLeft.id))
    assertEquals(tombstone.resolution, Some("propagated"))
    assert(tombstone.resolved)
  }

  test("executor: skipped removal keeps the tombstone active and is never copied back") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val goneLeft = mkAsset("l-gone", "chk-gone")
    // Owner-native asset on the right: the sync user cannot remove it.
    val ownerNativeRight = mkAsset("r-native", "chk-gone")

    seedBaseline(repo, pair, Seq(goneLeft), Seq(ownerNativeRight))
    api.markNonRemovable(ownerNativeRight.id)
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(ownerNativeRight))

    runSync(repo, api, pair)

    // Removal attempted, but skipped: no album_remove deletion log, no trash.
    assertEquals(api.removeFromAlbumCalls.size, 1)
    assert(repo.deletions.isEmpty)
    assert(api.trashCalls.isEmpty)
    assertEquals(repo.events.count(_.eventType == "removal_skipped"), 1)
    // The tombstone stays ACTIVE: 'propagated' would let the photo flow back to left.
    val tombstone = repo.tombstones((pair.id, "left", goneLeft.id))
    assertEquals(tombstone.resolution, None)
    assert(!tombstone.resolved)

    // Next run: chk-gone still on right, absent from left — must NOT be copied back.
    runSync(repo, api, pair)
    assert(api.uploadCalls.isEmpty)
    assert(api.addToAlbumCalls.isEmpty)
  }

  test("executor: trashing a live photo still also reclaims its motion video") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val goneLeft = mkAsset("l-gone", "chk-still")
    val stillRight = mkAsset("r-still", "chk-still").copy(livePhotoVideoId = Some("r-video"))
    val videoRight = mkAsset("r-video", "chk-video")

    seedBaseline(repo, pair, Seq(goneLeft), Seq(stillRight))
    repo.seedUploadedAsset(rightPeer.id, stillRight.id, stillRight.checksum)
    repo.seedUploadedAsset(rightPeer.id, videoRight.id, videoRight.checksum)
    api.setAssetInfo(videoRight)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight))

    runSync(repo, api, pair)

    assertEquals(api.trashCalls.head._2.toSet, Set(stillRight.id, videoRight.id))
    assertEquals(repo.deletions.filter(_.action == "trash").map(_.assetId).toSet, Set(stillRight.id, videoRight.id))
  }

  test("executor: dry run downloads and uploads nothing while previewing the full plan") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(mode = "left_to_right", forceAdditive = true)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(mkAsset("l1", "c1"), mkAsset("l2", "c2")))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)

    runSync(repo, api, pair, applyWrites = false)

    // The plan was walked (event reports the would-be uploads) but no bytes moved.
    assert(api.assetGetCalls.isEmpty)
    assert(api.uploadCalls.isEmpty)
    assert(api.addToAlbumCalls.isEmpty)
    assert(repo.uploadedAssets.isEmpty)
    val event = repo.events.find(_.eventType == "direction_sync").get
    assert(event.payloadText.contains("\"uploaded\":2"))
    assert(event.payloadText.contains("\"apply_writes\":false"))
  }

  test("executor: dry runs never prune the baseline observations") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair
    val retention = RetentionConfig(observationKeepRuns = 2, auditRetentionDays = 90)

    val left = Seq(mkAsset("l1", "c1"))
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, left)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(mkAsset("r1", "c1")))

    runSync(repo, api, pair, applyWrites = true, retention = retention)
    val baselineRunId = repo.findPreviousBaselineRunId(pair.id).get

    // Far more dry cycles than the retention window: the baseline must survive.
    (1 to 5).foreach(_ => runSync(repo, api, pair, applyWrites = false, retention = retention))

    assertEquals(repo.findPreviousBaselineRunId(pair.id), Some(baselineRunId))
    assert(repo.getRunObservations(baselineRunId, "left").nonEmpty)

    // The next apply run still detects removals against that baseline.
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    runSync(repo, api, pair, applyWrites = true, retention = retention)
    assertEquals(repo.events.count(_.eventType == "removal_detected"), 1)
  }

  test("executor: no trash without provenance, favorites and archived are protected") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val goneLeftA = mkAsset("l-a", "chk-a")
    val goneLeftB = mkAsset("l-b", "chk-b")
    val rightA = mkAsset("r-a", "chk-a") // no provenance: user-originated
    val rightB = mkAsset("r-b", "chk-b", isFavorite = true) // provenance but favorited

    seedBaseline(repo, pair, Seq(goneLeftA, goneLeftB), Seq(rightA, rightB))
    repo.seedUploadedAsset(rightPeer.id, rightB.id, rightB.checksum)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(rightA, rightB))

    runSync(repo, api, pair)

    assertEquals(api.removeFromAlbumCalls.size, 1)
    assertEquals(api.removeFromAlbumCalls.head._3.toSet, Set(rightA.id, rightB.id))
    assert(api.trashCalls.isEmpty)
    assertEquals(repo.deletions.count(_.action == "trash"), 0)
  }

  test("executor: asset still referenced by another album is not trashed") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val goneLeft = mkAsset("l-gone", "chk-gone")
    val stillRight = mkAsset("r-gone", "chk-gone")

    seedBaseline(repo, pair, Seq(goneLeft), Seq(stillRight))
    repo.seedUploadedAsset(rightPeer.id, stillRight.id, stillRight.checksum)
    api.setAlbumsContaining(stillRight.id, Seq("some-other-album"))

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight))

    runSync(repo, api, pair)

    assertEquals(api.removeFromAlbumCalls.size, 1)
    assert(api.trashCalls.isEmpty)
  }

  test("executor: suppressed removal is not propagated and not copied back on later runs") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = false)

    val goneLeft = mkAsset("l-gone", "chk-gone")
    val stillRight = mkAsset("r-gone", "chk-gone")

    seedBaseline(repo, pair, Seq(goneLeft), Seq(stillRight))
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight))

    runSync(repo, api, pair)

    assert(api.removeFromAlbumCalls.isEmpty)
    assert(api.trashCalls.isEmpty)
    assert(api.uploadCalls.isEmpty)
    val tombstone = repo.tombstones((pair.id, "left", goneLeft.id))
    assertEquals(tombstone.resolution, None)
    assert(!tombstone.resolved)

    // Second run: the baseline no longer contains chk-gone, only the active tombstone
    // keeps it from flowing back to left.
    runSync(repo, api, pair)
    assert(api.uploadCalls.isEmpty)
    assert(api.addToAlbumCalls.isEmpty)
  }

  test("executor: re-added photo on the origin side resolves the tombstone and flows again") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair

    val reAddedLeft = mkAsset("l-again", "chk-x")
    seedBaseline(repo, pair, Seq.empty, Seq.empty)
    repo.seedTombstone(pair.id, "left", "l-old", "chk-x")

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(reAddedLeft))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)

    runSync(repo, api, pair)

    val tombstone = repo.tombstones((pair.id, "left", "l-old"))
    assertEquals(tombstone.resolution, Some("readded"))
    assertEquals(api.uploadCalls.size, 1)
  }

  test("executor: mass removal quarantines the pair and applies nothing") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val baselineLeft = (1 to 30).map(i => mkAsset(s"l$i", s"c$i"))
    val baselineRight = (1 to 30).map(i => mkAsset(s"r$i", s"c$i"))
    seedBaseline(repo, pair, baselineLeft, baselineRight)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, baselineRight)

    runSync(repo, api, pair)

    val runId = repo.runs.keys.max
    assertEquals(repo.runs(runId).status, "quarantined")
    assertEquals(repo.quarantined.keySet, Set(pair.id))
    // A one-shot rearm key was generated for the incident.
    assert(repo.rearmKeys.get(pair.id).exists(_.nonEmpty))
    assert(api.removeFromAlbumCalls.isEmpty)
    assert(api.trashCalls.isEmpty)
    assert(api.uploadCalls.isEmpty)
    assert(api.addToAlbumCalls.isEmpty)
    // No observations for a quarantined run: the baseline stays untouched.
    assert(repo.observations.filter(_.runId == runId).isEmpty)
    assertEquals(repo.events.count(_.eventType == "quarantine"), 1)
  }

  test("executor: dry run reports quarantine but does not mark the pair") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val baselineLeft = (1 to 30).map(i => mkAsset(s"l$i", s"c$i"))
    seedBaseline(repo, pair, baselineLeft, Seq.empty)
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)

    runSync(repo, api, pair, applyWrites = false)

    val runId = repo.runs.keys.max
    assertEquals(repo.runs(runId).status, "quarantined")
    assert(repo.quarantined.isEmpty)
  }

  test("executor: failed fetch records no observations and a null-safe message") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()

    api.failAlbumGet(leftPeer.baseUrl, basePair.leftAlbumId, RuntimeException("boom"))

    intercept[RuntimeException] {
      runSync(repo, api)
    }
    assertEquals(repo.runs(1L).status, "failed")
    assertEquals(repo.runs(1L).message, Some("boom"))
    assert(repo.observations.isEmpty)

    api.failAlbumGet(leftPeer.baseUrl, basePair.leftAlbumId, NullPointerException())
    intercept[NullPointerException] {
      runSync(repo, api)
    }
    assertEquals(repo.runs(2L).status, "failed")
    assert(repo.runs(2L).message.exists(_.contains("NullPointerException")))
  }

  test("executor: dry runs record observations but never sync state, and never become baseline") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(propagateDeletes = true)

    val goneLeft = mkAsset("l-gone", "chk-gone")
    val stillRight = mkAsset("r-gone", "chk-gone")
    seedBaseline(repo, pair, Seq(goneLeft), Seq(stillRight))
    val baselineRunId = repo.findPreviousBaselineRunId(pair.id).get

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq.empty)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(stillRight))

    runSync(repo, api, pair, applyWrites = false)

    val dryRunId = repo.runs.keys.max
    assertEquals(repo.runs(dryRunId).status, "success")
    assertEquals(repo.runs(dryRunId).dryRun, true)
    // Nothing was written to Immich, and no sync state changed.
    assert(api.removeFromAlbumCalls.isEmpty)
    assert(repo.tombstones.isEmpty)
    assert(repo.deletions.isEmpty)
    // Observations exist for audit, but the baseline still points at the apply run.
    assert(repo.observations.exists(_.runId == dryRunId))
    assertEquals(repo.findPreviousBaselineRunId(pair.id), Some(baselineRunId))
    // The removal was still reported.
    assertEquals(repo.events.count(_.eventType == "removal_detected"), 1)
    val removalEvents = repo.events.filter(_.eventType == "removal_propagated")
    assertEquals(removalEvents.size, 1)
    assert(removalEvents.head.payloadText.contains("\"apply_writes\":false"))
  }

  test("executor: first apply run clears force_additive") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair.copy(forceAdditive = true)

    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, Seq(mkAsset("l1", "c1")))
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq.empty)

    runSync(repo, api, pair, applyWrites = false)
    assert(repo.forceAdditiveCleared.isEmpty)

    runSync(repo, api, pair, applyWrites = true)
    assertEquals(repo.forceAdditiveCleared.toSet, Set(pair.id))
  }

  test("executor: observation retention keeps rows bounded to K runs") {
    val repo = InMemorySyncRepository()
    val api = FakeImmichApi()
    val pair = basePair
    val retention = RetentionConfig(observationKeepRuns = 2, auditRetentionDays = 90)

    val left = Seq(mkAsset("l1", "c1"), mkAsset("l2", "c2"))
    api.setAlbumAssets(leftPeer.baseUrl, pair.leftAlbumId, left)
    api.setAlbumAssets(rightPeer.baseUrl, pair.rightAlbumId, Seq(mkAsset("r1", "c1"), mkAsset("r2", "c2")))

    (1 to 6).foreach(_ => runSync(repo, api, pair, retention = retention))

    val runIdsWithObservations = repo.observations.map(_.runId).distinct
    assertEquals(runIdsWithObservations.size, 2)
    // The newest run (the baseline) always has its observations.
    val baselineRunId = repo.findPreviousBaselineRunId(pair.id).get
    assert(runIdsWithObservations.contains(baselineRunId))
    // Bounded: K runs x (left + right) rows.
    assertEquals(repo.observations.size, 2 * 4)
  }
