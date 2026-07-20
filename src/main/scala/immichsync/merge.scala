package immichsync

// Merge planner (pure): baseline 3-way merge by content checksum.
//
// Each run diffs both sides against the last agreed baseline (the observations of
// the previous successful apply run). Adds flow both ways (mode permitting);
// removals only propagate in the same run they were detected, and only when the
// pair opts in. Conflicts default to add-wins: a photo removed on one side but
// newly added on the other survives. No timestamps take part in any decision.

def computeMergePlan(in: MergeInput): MergePlan =
  val mode = in.pair.mode.toLowerCase
  val flowLeftToRight = mode == "bidirectional" || mode == "left_to_right"
  val flowRightToLeft = mode == "bidirectional" || mode == "right_to_left"
  if (!flowLeftToRight && !flowRightToLeft)
    throw new RuntimeException(s"Unsupported pair mode '${in.pair.mode}' for pair '${in.pair.name}'")

  val activeLeft = in.leftAssets.filterNot(_.isTrashed)
  val activeRight = in.rightAssets.filterNot(_.isTrashed)
  val leftByChecksum = activeLeft.groupBy(_.checksum)
  val rightByChecksum = activeRight.groupBy(_.checksum)
  val curL = leftByChecksum.keySet
  val curR = rightByChecksum.keySet
  val trashedLeft = in.leftAssets.filter(_.isTrashed).map(_.checksum).toSet -- curL
  val trashedRight = in.rightAssets.filter(_.isTrashed).map(_.checksum).toSet -- curR

  // force_additive treats the world as if there were no baseline and no tombstones:
  // the run becomes a pure additive union, which is the only safe interpretation of a
  // fresh link, a re-arm, or a stale baseline.
  val baselineLeftRows = if (in.forceAdditive) Vector.empty else in.baselineLeft.filterNot(_.isTrashed)
  val baselineRightRows = if (in.forceAdditive) Vector.empty else in.baselineRight.filterNot(_.isTrashed)
  val baseL = baselineLeftRows.map(_.checksum).toSet
  val baseR = baselineRightRows.map(_.checksum).toSet
  val suppressedL =
    if (in.forceAdditive) Set.empty[String]
    else in.activeTombstones.filter(_.originSide == "left").map(_.checksum).toSet
  val suppressedR =
    if (in.forceAdditive) Set.empty[String]
    else in.activeTombstones.filter(_.originSide == "right").map(_.checksum).toSet

  val addsL = curL -- baseL
  val addsR = curR -- baseR
  val removalsL = baseL -- curL
  val removalsR = baseR -- curR

  def breach(side: String, removals: Set[String], baseSize: Int): Option[String] =
    if (removals.size > in.thresholds.maxRemovalCount)
      Some(s"$side album lost ${removals.size} assets in one run (max ${in.thresholds.maxRemovalCount})")
    else if (
      removals.size >= Thresholds.FractionMinRemovals && baseSize > 0 &&
      removals.size.toDouble / baseSize > in.thresholds.maxRemovalFraction
    )
      Some(
        s"$side album lost ${removals.size} of $baseSize assets " +
          s"(${math.round(removals.size.toDouble / baseSize * 100)}% > ${math.round(in.thresholds.maxRemovalFraction * 100)}%)"
      )
    else None

  val quarantineReason = breach("left", removalsL, baseL.size).orElse(breach("right", removalsR, baseR.size))
  if (quarantineReason.isDefined) MergePlan.quarantined(quarantineReason.get)
  else {
    // Lifecycle of persisted (active) tombstones.
    val readdedL = suppressedL & curL
    val readdedR = suppressedR & curR
    val addWinsIntoL = if (flowRightToLeft) (suppressedL -- curL) & addsR else Set.empty[String]
    val addWinsIntoR = if (flowLeftToRight) (suppressedR -- curR) & addsL else Set.empty[String]

    val resolutions =
      readdedL.toSeq.sorted.map(TombstoneResolutionPlan("left", _, "readded")) ++
        readdedR.toSeq.sorted.map(TombstoneResolutionPlan("right", _, "readded")) ++
        (addWinsIntoL -- readdedL).toSeq.sorted.map(TombstoneResolutionPlan("left", _, "add_wins")) ++
        (addWinsIntoR -- readdedR).toSeq.sorted.map(TombstoneResolutionPlan("right", _, "add_wins"))

    // Fresh removals detected this run. A removal only ever propagates in the same run it
    // was detected against a fresh baseline; older suppressed tombstones stay suppress-only
    // even if propagate_deletes is enabled later.
    def freshRemovals(
        originSide: String,
        removals: Set[String],
        baselineRows: Vector[ObservationRow],
        otherCur: Set[String],
        otherAdds: Set[String],
        flowOtherToOrigin: Boolean,
        flowOriginToOther: Boolean,
        trashedChecksums: Set[String],
    ): Seq[TombstoneWrite] =
      removals.toSeq.sorted.flatMap { checksum =>
        val resolution =
          if (otherAdds.contains(checksum) && flowOtherToOrigin) Some("add_wins")
          else if (!otherCur.contains(checksum)) Some("converged")
          else if (in.pair.propagateDeletes && flowOriginToOther) Some("propagated")
          else None
        val originAssetIds = baselineRows.filter(_.checksum == checksum).map(_.assetId).distinct
        originAssetIds.map { assetId =>
          TombstoneWrite(originSide, assetId, checksum, trashedChecksums.contains(checksum), resolution)
        }
      }

    val tombstonesL = freshRemovals(
      "left", removalsL, baselineLeftRows,
      otherCur = curR, otherAdds = addsR,
      flowOtherToOrigin = flowRightToLeft, flowOriginToOther = flowLeftToRight,
      trashedChecksums = trashedLeft,
    )
    val tombstonesR = freshRemovals(
      "right", removalsR, baselineRightRows,
      otherCur = curL, otherAdds = addsL,
      flowOtherToOrigin = flowLeftToRight, flowOriginToOther = flowRightToLeft,
      trashedChecksums = trashedRight,
    )

    val propagateOnRight = tombstonesL.filter(_.resolution.contains("propagated")).map(_.checksum).toSet
    val propagateOnLeft = tombstonesR.filter(_.resolution.contains("propagated")).map(_.checksum).toSet
    val removeFromRight = propagateOnRight.toSeq.sorted.flatMap(c => rightByChecksum.getOrElse(c, Seq.empty))
    val removeFromLeft = propagateOnLeft.toSeq.sorted.flatMap(c => leftByChecksum.getOrElse(c, Seq.empty))

    // Copies into a side are blocked by that side's own removals (fresh or persisted),
    // unless the other side newly added the photo (add-wins).
    val blockedIntoL =
      tombstonesL.filterNot(_.resolution.contains("add_wins")).map(_.checksum).toSet ++
        (suppressedL -- readdedL -- addWinsIntoL)
    val blockedIntoR =
      tombstonesR.filterNot(_.resolution.contains("add_wins")).map(_.checksum).toSet ++
        (suppressedR -- readdedR -- addWinsIntoR)

    val copyLeftToRight =
      if (flowLeftToRight) (curL -- curR -- blockedIntoR).toSeq.sorted.map(c => leftByChecksum(c).minBy(_.id))
      else Seq.empty
    val copyRightToLeft =
      if (flowRightToLeft) (curR -- curL -- blockedIntoL).toSeq.sorted.map(c => rightByChecksum(c).minBy(_.id))
      else Seq.empty

    MergePlan(
      copyLeftToRight = copyLeftToRight,
      copyRightToLeft = copyRightToLeft,
      removeFromLeft = removeFromLeft,
      removeFromRight = removeFromRight,
      tombstoneWrites = tombstonesL ++ tombstonesR,
      resolutions = resolutions,
      quarantineReason = None,
    )
  }
