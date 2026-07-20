package immichsync

// Read-only facades: dry-run safety by construction, not by scattered flag checks.
//
// The executor and discovery wrap their ImmichApi / SyncRepository in these when not
// applying, so no code path can mutate an Immich instance or the sync state even if a
// guard is forgotten somewhere. Reads delegate; writes log what WOULD happen. The full
// plan is still walked (bulk checks, removal results, upload candidates), which makes
// the dry-run preview complete instead of zeroed out.

class DryRunImmichApi(underlying: ImmichApi) extends ImmichApi:
  private def would(action: String): Unit = println(s"[dry-run] would $action")

  override def serverVersion(server: ImmichServer): String = underlying.serverVersion(server)
  override def listAlbums(server: ImmichServer): Seq[AlbumSummary] = underlying.listAlbums(server)
  override def albumGetAssets(album: Album): Seq[AssetResponseDto] = underlying.albumGetAssets(album)
  override def assetInfo(server: ImmichServer, assetId: String): AssetResponseDto = underlying.assetInfo(server, assetId)
  override def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp] =
    underlying.assetBulkCheck(server, assets)
  override def albumsContainingAsset(server: ImmichServer, assetId: String): Seq[String] =
    underlying.albumsContainingAsset(server, assetId)

  // Originals are not downloaded in a preview: uploads are no-ops anyway.
  override def assetGet(server: ImmichServer, assetId: String): Array[Byte] = Array.emptyByteArray

  override def updateAlbumDescription(album: Album, description: String): Unit =
    would(s"update description of album ${album.id}")

  override def untrash(server: ImmichServer, assetIds: Seq[String]): Unit =
    would(s"restore ${assetIds.size} asset(s) from trash")

  override def albumAddAssets(album: Album, assetIds: Seq[String]): Unit =
    would(s"add ${assetIds.size} asset(s) to album ${album.id}")

  override def albumRemoveAssets(album: Album, assetIds: Seq[String]): AlbumRemoveResult =
    would(s"remove ${assetIds.size} asset(s) from album ${album.id}")
    // Pretend everything is removable: the preview shows the intended outcome; real
    // permission skips only exist against a real API.
    AlbumRemoveResult(removed = assetIds)

  override def trashAssets(server: ImmichServer, assetIds: Seq[String]): Unit =
    would(s"trash ${assetIds.size} orphaned asset(s)")

  override def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): UploadResult =
    would(s"upload '${uploadRequest.filename}'")
    // Not "created": provenance is never recorded for a pretend upload.
    UploadResult(id = s"dry-run-${uploadRequest.checksum.getOrElse(uploadRequest.filename)}", status = "dry-run")

class DryRunSyncRepository(underlying: SyncRepository) extends SyncRepository:
  // Run bookkeeping and audit stay real (runs are marked dry_run and never become
  // baselines); everything that influences future sync decisions is dropped.
  override def startRun(pairId: Long, dryRun: Boolean): Long = underlying.startRun(pairId, dryRun = true)
  override def completeRun(runId: Long, status: String, message: Option[String]): Unit =
    underlying.completeRun(runId, status, message)
  override def findPreviousBaselineRunId(pairId: Long): Option[Long] = underlying.findPreviousBaselineRunId(pairId)
  override def getRunObservations(runId: Long, side: String): Vector[ObservationRow] =
    underlying.getRunObservations(runId, side)
  override def loadActiveTombstones(pairId: Long): Vector[ActiveTombstone] = underlying.loadActiveTombstones(pairId)
  override def recordSyncEvent(runId: Long, pairId: Long, eventType: String, direction: String, assetCount: Int, payloadText: String): Unit =
    underlying.recordSyncEvent(runId, pairId, eventType, direction, assetCount, payloadText)
  override def uploadedByTool(peerId: Long, assetIds: Seq[String]): Set[String] =
    underlying.uploadedByTool(peerId, assetIds)

  override def recordUploadedAsset(runId: Long, pairId: Long, peerId: Long, assetId: String, checksum: String): Unit = ()
  override def recordDeletion(runId: Long, pairId: Long, peerId: Long, albumId: String, assetId: String, checksum: String, action: String): Unit = ()
  override def markQuarantined(pairId: Long, reason: String, rearmKey: String): Unit = ()

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
    // Observations are audit and welcome; tombstones/resolutions/force_additive/pruning
    // are sync state and dropped regardless of what the caller passed.
    underlying.finalizeRun(runId, pair, Seq.empty, Seq.empty, observationsLeft, observationsRight, applyRun = false, observationKeepRuns)
