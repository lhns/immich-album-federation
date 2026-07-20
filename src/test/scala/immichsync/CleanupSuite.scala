package immichsync

import java.time.OffsetDateTime

class CleanupSuite extends munit.FunSuite:
  private val now = OffsetDateTime.parse("2026-07-20T12:00:00Z")

  private def row(assetId: String, ageHours: Long): ProvenanceRow =
    ProvenanceRow(assetId, s"chk-$assetId", now.minusHours(ageHours))

  private def facts(
      provenance: Vector[ProvenanceRow],
      albumMemberIds: Set[String] = Set.empty,
      livePhotoVideoIds: Set[String] = Set.empty,
      afterDays: Int = 1,
      maxPerPass: Int = 0,
  ): CleanupFacts =
    CleanupFacts(provenance, albumMemberIds, livePhotoVideoIds, now, afterDays, maxPerPass)

  test("age gate: only assets older than afterDays qualify") {
    val selection = selectCleanupCandidates(facts(Vector(row("fresh", 23), row("old", 25))))
    assertEquals(selection.candidates.map(_.assetId), Seq("old"))
    assertEquals(selection.cappedRemainder, 0)
  }

  test("album members are never candidates") {
    val selection = selectCleanupCandidates(facts(
      Vector(row("in-album", 48), row("orphan", 48)),
      albumMemberIds = Set("in-album"),
    ))
    assertEquals(selection.candidates.map(_.assetId), Seq("orphan"))
  }

  test("motion videos of album-resident live photos are never candidates") {
    val selection = selectCleanupCandidates(facts(
      Vector(row("video-of-live-still", 48), row("orphan-video", 48)),
      livePhotoVideoIds = Set("video-of-live-still"),
    ))
    assertEquals(selection.candidates.map(_.assetId), Seq("orphan-video"))
  }

  test("cap takes the oldest first and reports the remainder") {
    val selection = selectCleanupCandidates(facts(
      Vector(row("a", 30), row("b", 50), row("c", 40)),
      maxPerPass = 2,
    ))
    assertEquals(selection.candidates.map(_.assetId), Seq("b", "c"))
    assertEquals(selection.cappedRemainder, 1)
  }

  test("no provenance means nothing to do") {
    val selection = selectCleanupCandidates(facts(Vector.empty))
    assert(selection.candidates.isEmpty)
    assertEquals(selection.cappedRemainder, 0)
  }
