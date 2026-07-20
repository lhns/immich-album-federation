package immichsync

// Sync executor: fetch both sides, plan (pure merge), apply, finalize.

def applyAdds(
    repo: SyncRepository,
    api: ImmichApi,
    runId: Long,
    pair: AlbumPair,
    direction: String,
    sourceServer: ImmichServer,
    targetServer: ImmichServer,
    targetAlbum: Album,
    targetPeerId: Long,
    candidates: Seq[AssetResponseDto],
    applyWrites: Boolean,
    transferConcurrency: Int,
): Unit =
  if (candidates.nonEmpty) {
    val bulkCheckResults = api.assetBulkCheck(targetServer, candidates)
    val (existingResults, nonExistingResults) = bulkCheckResults.partition(_.assetId.isDefined)
    val toUntrash = existingResults.filter(_.isTrashed).flatMap(_.assetId)

    if (toUntrash.nonEmpty && applyWrites) {
      api.untrash(targetServer, toUntrash)
    }

    def uploadOne(dto: AssetResponseDto, livePhotoVideoId: Option[String]): UploadResult =
      val bytes = api.assetGet(sourceServer, dto.id)
      val uploadRequest = AssetUploadRequest
        .fromAssetResponseDto(dto, bytes)
        .copy(livePhotoVideoId = livePhotoVideoId)
      val result = api.assetUpload(targetServer, uploadRequest)
      // Provenance is recorded immediately (not in the final transaction) so a crash
      // later in the run can never lose track of an asset this tool created.
      if (result.created) {
        repo.recordUploadedAsset(runId, pair.id, targetPeerId, result.id, dto.checksum)
      }
      result

    val uploadedIds =
      if (applyWrites) {
        parMap(nonExistingResults, transferConcurrency) { dto =>
          // Live photos: transfer the motion video first, then link the still to it.
          val targetVideoId = dto.asset.livePhotoVideoId.map { videoId =>
            val videoDto = api.assetInfo(sourceServer, videoId)
            uploadOne(videoDto, livePhotoVideoId = None).id
          }
          uploadOne(dto.asset, livePhotoVideoId = targetVideoId).id
        }
      } else {
        Vector.empty[String]
      }

    val addToAlbumIds =
      if (applyWrites) {
        uploadedIds ++ existingResults.flatMap(_.assetId)
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

// Applies membership removals best-effort and returns the checksums that could NOT be
// fully removed (the sync user may only remove assets it owns; owner-native assets on
// the target are skipped with a warning).
def applyRemovals(
    repo: SyncRepository,
    api: ImmichApi,
    runId: Long,
    pair: AlbumPair,
    direction: String,
    targetServer: ImmichServer,
    targetAlbum: Album,
    targetPeerId: Long,
    removals: Seq[AssetResponseDto],
    applyWrites: Boolean,
): Set[String] =
  if (removals.isEmpty) Set.empty
  else {
    val byId = removals.map(a => a.id -> a).toMap
    var skippedChecksums = Set.empty[String]
    var trashedCount = 0
    var removedCount = 0

    if (applyWrites) {
      val result = api.albumRemoveAssets(targetAlbum, removals.map(_.id))
      removedCount = result.removed.size
      result.removed.flatMap(byId.get).foreach { asset =>
        repo.recordDeletion(runId, pair.id, targetPeerId, targetAlbum.id, asset.id, asset.checksum, "album_remove")
      }

      if (result.skipped.nonEmpty) {
        skippedChecksums = result.skipped.flatMap((id, _) => byId.get(id)).map(_.checksum).toSet
        result.skipped.foreach { (id, error) =>
          System.err.println(s"[pair=${pair.name}] cannot remove asset $id from ${targetAlbum.id}: $error (owner-native asset?)")
        }
        repo.recordSyncEvent(
          runId = runId,
          pairId = pair.id,
          eventType = "removal_skipped",
          direction = direction,
          assetCount = result.skipped.size,
          payloadText = ujson.Obj(
            "skipped" -> result.skipped.map((id, error) => ujson.Obj("assetId" -> id, "error" -> error)),
          ).render(),
        )
      }

      if (pair.trashOrphanedAssets) {
        // Trash-eligible only when: this tool created the asset (uploaded_asset provenance),
        // the user has not marked it (favorite/archive), no album still references it, and
        // the membership removal above actually succeeded.
        val removedAssets = result.removed.flatMap(byId.get)
        val unmarked = removedAssets.filterNot(a => a.isFavorite || a.isArchived)
        val ours = repo.uploadedByTool(targetPeerId, unmarked.map(_.id))
        val toTrash = unmarked.filter(a => ours.contains(a.id) && api.albumsContainingAsset(targetServer, a.id).isEmpty)
        if (toTrash.nonEmpty) {
          api.trashAssets(targetServer, toTrash.map(_.id))
          toTrash.foreach { asset =>
            repo.recordDeletion(runId, pair.id, targetPeerId, targetAlbum.id, asset.id, asset.checksum, "trash")
          }
          trashedCount = toTrash.size
        }
      }
    }

    val payload = ujson.Obj(
      "album_removed" -> removedCount,
      "skipped" -> skippedChecksums.size,
      "trashed" -> trashedCount,
      "apply_writes" -> applyWrites,
    ).render()
    repo.recordSyncEvent(
      runId = runId,
      pairId = pair.id,
      eventType = "removal_propagated",
      direction = direction,
      assetCount = removals.size,
      payloadText = payload,
    )

    skippedChecksums
  }

def executePairSync(
    db: DbRuntime,
    api: ImmichApi,
    pair: AlbumPair,
    leftPeer: SyncPeer,
    rightPeer: SyncPeer,
    safety: SafetyConfig,
    applyWrites: Boolean,
    thresholds: Thresholds,
    transferConcurrency: Int,
    retention: RetentionConfig,
): Unit =
  executePairSyncWith(
    pair = pair,
    leftPeer = leftPeer,
    rightPeer = rightPeer,
    safety = safety,
    applyWrites = applyWrites,
    repo = DbSyncRepository(db),
    api = api,
    resolveApiKey = requiredEnv,
    thresholds = thresholds,
    transferConcurrency = transferConcurrency,
    retention = retention,
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
    thresholds: Thresholds = Thresholds.Default,
    transferConcurrency: Int = 3,
    retention: RetentionConfig = RetentionConfig.Default,
): Unit =
  assertSafeHost(leftPeer.baseUrl, safety)
  assertSafeHost(rightPeer.baseUrl, safety)

  val leftServer = ImmichServer(leftPeer.baseUrl, resolveApiKey(leftPeer.apiKeyEnv))
  val rightServer = ImmichServer(rightPeer.baseUrl, resolveApiKey(rightPeer.apiKeyEnv))
  val leftAlbum = Album(leftServer, pair.leftAlbumId)
  val rightAlbum = Album(rightServer, pair.rightAlbumId)

  val runId = repo.startRun(pair.id, dryRun = !applyWrites)

  try {
    // Any fetch failure throws here: a failed run records no observations, so an HTTP
    // error can never masquerade as an empty album and poison the baseline.
    val (leftAssets, rightAssets) = par2(api.albumGetAssets(leftAlbum), api.albumGetAssets(rightAlbum))

    val baselineRunId = repo.findPreviousBaselineRunId(pair.id)
    val forceAdditive = pair.forceAdditive || baselineRunId.isEmpty
    val baselineLeft = baselineRunId.map(repo.getRunObservations(_, "left")).getOrElse(Vector.empty)
    val baselineRight = baselineRunId.map(repo.getRunObservations(_, "right")).getOrElse(Vector.empty)
    val activeTombstones = repo.loadActiveTombstones(pair.id)

    val pairThresholds = Thresholds(
      maxRemovalCount = pair.maxRemovalCount.getOrElse(thresholds.maxRemovalCount),
      maxRemovalFraction = pair.maxRemovalFraction.getOrElse(thresholds.maxRemovalFraction),
    )

    val plan = computeMergePlan(MergeInput(
      pair = pair,
      baselineLeft = baselineLeft,
      baselineRight = baselineRight,
      leftAssets = leftAssets,
      rightAssets = rightAssets,
      activeTombstones = activeTombstones,
      thresholds = pairThresholds,
      forceAdditive = forceAdditive,
    ))

    plan.quarantineReason match {
      case Some(reason) =>
        repo.recordSyncEvent(
          runId = runId,
          pairId = pair.id,
          eventType = "quarantine",
          direction = "both",
          assetCount = 0,
          payloadText = ujson.Obj("reason" -> reason, "apply_writes" -> applyWrites).render(),
        )
        // A dry run reports the breach but does not mutate pair state.
        if (applyWrites) repo.markQuarantined(pair.id, reason)
        repo.completeRun(runId, status = "quarantined", message = Some(reason))

      case None =>
        val freshRemovalSides = plan.tombstoneWrites.groupBy(_.originSide)
        freshRemovalSides.toSeq.sortBy(_._1).foreach { (side, writes) =>
          repo.recordSyncEvent(
            runId = runId,
            pairId = pair.id,
            eventType = "removal_detected",
            direction = side,
            assetCount = writes.map(_.checksum).distinct.size,
            payloadText = ujson.Obj(
              "via_trash" -> writes.count(_.viaTrash),
              "propagate_deletes" -> pair.propagateDeletes,
              "force_additive" -> forceAdditive,
            ).render(),
          )
        }

        applyAdds(
          repo, api, runId, pair,
          direction = "left_to_right",
          sourceServer = leftServer, targetServer = rightServer,
          targetAlbum = rightAlbum, targetPeerId = rightPeer.id,
          candidates = plan.copyLeftToRight,
          applyWrites = applyWrites,
          transferConcurrency = transferConcurrency,
        )
        applyAdds(
          repo, api, runId, pair,
          direction = "right_to_left",
          sourceServer = rightServer, targetServer = leftServer,
          targetAlbum = leftAlbum, targetPeerId = leftPeer.id,
          candidates = plan.copyRightToLeft,
          applyWrites = applyWrites,
          transferConcurrency = transferConcurrency,
        )

        val skippedOnRight = applyRemovals(
          repo, api, runId, pair,
          direction = "left_to_right",
          targetServer = rightServer, targetAlbum = rightAlbum, targetPeerId = rightPeer.id,
          removals = plan.removeFromRight,
          applyWrites = applyWrites,
        )
        val skippedOnLeft = applyRemovals(
          repo, api, runId, pair,
          direction = "right_to_left",
          targetServer = leftServer, targetAlbum = leftAlbum, targetPeerId = leftPeer.id,
          removals = plan.removeFromLeft,
          applyWrites = applyWrites,
        )

        // A removal that could not be fully applied must keep its tombstone ACTIVE:
        // marking it 'propagated' would let the next run copy the photo back to the
        // side that deleted it. (Left-origin removals apply on the right and vice versa.)
        val tombstoneWrites = plan.tombstoneWrites.map { write =>
          val skipped =
            (write.originSide == "left" && skippedOnRight.contains(write.checksum)) ||
              (write.originSide == "right" && skippedOnLeft.contains(write.checksum))
          if (skipped && write.resolution.contains("propagated")) write.copy(resolution = None)
          else write
        }

        // Dry runs record observations for audit but leave sync state (tombstones,
        // resolutions, force_additive) untouched: baselines exclude dry runs anyway.
        repo.finalizeRun(
          runId = runId,
          pair = pair,
          tombstoneWrites = if (applyWrites) tombstoneWrites else Seq.empty,
          resolutions = if (applyWrites) plan.resolutions else Seq.empty,
          observationsLeft = leftAssets.map(a => ObservationRow(a.id, a.checksum, a.isTrashed)),
          observationsRight = rightAssets.map(a => ObservationRow(a.id, a.checksum, a.isTrashed)),
          clearForceAdditive = applyWrites,
          observationKeepRuns = retention.observationKeepRuns,
        )
    }
  } catch {
    case e: Throwable =>
      val message = Option(e.getMessage).getOrElse(e.toString)
      repo.completeRun(runId, status = "failed", message = Some(message.take(1000)))
      throw e
  }
