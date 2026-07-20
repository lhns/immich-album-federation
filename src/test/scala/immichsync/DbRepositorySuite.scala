package immichsync

// Integration tests against H2 in PostgreSQL compatibility mode: runs the real Flyway
// migrations and the real DbSyncRepository / discovery / config SQL. The in-memory
// fakes in SyncSuite stay the fast default; this suite proves the SQL itself.

import com.augustnagro.magnum.*

import java.util.concurrent.atomic.AtomicLong

class DbRepositorySuite extends munit.FunSuite:
  private val dbCounter = AtomicLong(0)

  private val leftPeerCfg = PeerConfig("left", "http://left.local", "left-key")
  private val rightPeerCfg = PeerConfig("right", "http://right.local", "right-key")

  private def withDb[A](f: DbRuntime => A): A =
    val name = s"synctest${dbCounter.incrementAndGet()}"
    val db = createDbRuntime(
      url = s"jdbc:h2:mem:$name;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
      user = "sa",
      password = "",
    )
    try {
      runMigrations(db)
      f(db)
    } finally db.dataSource.close()

  private def seedPeers(db: DbRuntime): (SyncPeer, SyncPeer) =
    transact(db.xa):
      upsertPeer(leftPeerCfg)
      upsertPeer(rightPeerCfg)
    val peers = connect(db.xa)(loadEnabledPeers())
    (peers.find(_.name == "left").get, peers.find(_.name == "right").get)

  private def seedAnnotationPair(db: DbRuntime, leftPeer: SyncPeer, rightPeer: SyncPeer): AlbumPair =
    transact(db.xa):
      insertAnnotationPair(
        "auto/left-right/album-le-album-ri",
        DiscoveredLink(leftPeer.id, "album-left", rightPeer.id, "album-right", "bidirectional", propagateDeletes = true),
      )
    connect(db.xa)(loadAllPairs()).head

  private def mkAsset(id: String, checksum: String): AssetResponseDto =
    AssetResponseDto(
      checksum = checksum,
      id = id,
      fileCreatedAt = "2024-01-01T00:00:00.000Z",
      fileModifiedAt = "2024-01-01T00:00:00.000Z",
      originalFileName = s"$id.jpg",
    )

  private def runSync(db: DbRuntime, api: ImmichApi, pair: AlbumPair, leftPeer: SyncPeer, rightPeer: SyncPeer,
                      retention: RetentionConfig = RetentionConfig.Default): Unit =
    executePairSyncWith(
      pair = pair,
      leftPeer = leftPeer,
      rightPeer = rightPeer,
      applyWrites = true,
      repo = DbSyncRepository(db),
      api = api,
      resolveApiKey = _ => "dummy-key",
      retention = retention,
    )

  test("migrations apply cleanly and pair/peer loading works") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val pair = seedAnnotationPair(db, leftPeer, rightPeer)
      assertEquals(pair.linkSource, "annotation")
      assert(pair.forceAdditive)
      assert(pair.propagateDeletes)
      assertEquals(connect(db.xa)(loadEnabledPairs(None)).size, 1)
      assertEquals(connect(db.xa)(loadEnabledPairs(Some(pair.name))).size, 1)
    }
  }

  test("executor end-to-end on H2: union, provenance, removal, deletion log, quarantine, rearm") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      var pair = seedAnnotationPair(db, leftPeer, rightPeer)
      val api = FakeImmichApi()

      // Run 1: union — c2 flows to the right, provenance recorded, force_additive cleared.
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq(mkAsset("a1", "c1"), mkAsset("a2", "c2")))
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1")))
      api.setAssetBytes("a2", Array[Byte](1, 2))
      api.enqueueUploadResult(UploadResult("t-c2", "created"))
      runSync(db, api, pair, leftPeer, rightPeer)

      assert(connect(db.xa)(uploadedAssetExists(rightPeer.id, "t-c2")))
      pair = connect(db.xa)(loadAllPairs()).head
      assert(!pair.forceAdditive)
      val baseline1 = connect(db.xa)(previousBaselineRunId(pair.id))
      assert(baseline1.isDefined)
      assertEquals(connect(db.xa)(loadRunObservations(baseline1.get, "left")).size, 2)

      // Run 2: right album now contains the uploaded copy — fresh converged baseline.
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1"), mkAsset("t-c2", "c2")))
      runSync(db, api, pair, leftPeer, rightPeer)

      // Run 3: left removes c2 — removal propagates, orphan is trashed, all logged.
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq(mkAsset("a1", "c1")))
      runSync(db, api, pair, leftPeer, rightPeer)

      assertEquals(api.removeFromAlbumCalls.last._3, Seq("t-c2"))
      assertEquals(api.trashCalls.last._2, Seq("t-c2"))
      val deletionActions = connect(db.xa):
        sql"SELECT action FROM deletion_log ORDER BY id".query[String].run()
      assertEquals(deletionActions, Vector("album_remove", "trash"))
      // Tombstone resolved as propagated: no active tombstones remain.
      assert(connect(db.xa)(selectActiveTombstones(pair.id)).isEmpty)
      val resolutions = connect(db.xa):
        sql"SELECT resolution FROM tombstone WHERE pair_id = ${pair.id}".query[Option[String]].run()
      assertEquals(resolutions, Vector(Some("propagated")))

      // Run 4: force a quarantine via the per-pair override, then re-arm.
      transact(db.xa):
        sql"UPDATE album_pair SET max_removal_count = 0 WHERE id = ${pair.id}".update.run()
      pair = connect(db.xa)(loadAllPairs()).head
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1")))
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq.empty)
      runSync(db, api, pair, leftPeer, rightPeer)

      assertEquals(connect(db.xa)(loadEnabledPairs(None)).size, 0) // quarantined pairs excluded
      val quarantineReason = connect(db.xa):
        sql"SELECT quarantine_reason FROM album_pair WHERE id = ${pair.id}".query[Option[String]].run().head
      assert(quarantineReason.exists(_.contains("lost")))

      val (rearmed, cleared) = transact(db.xa):
        rearmPairByName(pair.name)
      assertEquals(rearmed, 1)
      assertEquals(cleared, 0) // the only tombstone was already resolved
      val reloaded = connect(db.xa)(loadAllPairs()).head
      assert(reloaded.forceAdditive)
      assertEquals(connect(db.xa)(loadEnabledPairs(None)).size, 1)
    }
  }

  test("observation retention prunes old runs' rows but never the baseline") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val pair0 = seedAnnotationPair(db, leftPeer, rightPeer)
      val api = FakeImmichApi()
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq(mkAsset("a1", "c1")))
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1")))
      val retention = RetentionConfig(observationKeepRuns = 2, auditRetentionDays = 90)

      (1 to 5).foreach { i =>
        val pair = connect(db.xa)(loadAllPairs()).head
        runSync(db, api, pair, leftPeer, rightPeer, retention = retention)
      }

      val runsWithObservations = connect(db.xa):
        sql"SELECT DISTINCT run_id FROM asset_observation".query[Long].run()
      assertEquals(runsWithObservations.size, 2)
      val baseline = connect(db.xa)(previousBaselineRunId(pair0.id)).get
      assert(runsWithObservations.contains(baseline))
    }
  }

  test("audit retention prunes old runs and resolved tombstones, protects everything else") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val pair = seedAnnotationPair(db, leftPeer, rightPeer)

      // Two apply runs (the newer is the baseline) + deletion log + tombstones.
      val repo = DbSyncRepository(db)
      val oldRun = repo.startRun(pair.id, dryRun = false)
      repo.finalizeRun(oldRun, pair, Seq.empty, Seq.empty, Seq(ObservationRow("a1", "c1", false)), Seq.empty, applyRun = true)
      val baselineRun = repo.startRun(pair.id, dryRun = false)
      repo.finalizeRun(baselineRun, pair, Seq.empty, Seq.empty, Seq(ObservationRow("a1", "c1", false)), Seq.empty, applyRun = true)

      repo.recordDeletion(oldRun, pair.id, rightPeer.id, "album-right", "x1", "cX", "album_remove")
      repo.recordUploadedAsset(oldRun, pair.id, rightPeer.id, "x1", "cX")
      transact(db.xa):
        upsertTombstone(pair.id, TombstoneWrite("left", "gone-1", "cGone", viaTrash = false, resolution = Some("propagated")), true)
        upsertTombstone(pair.id, TombstoneWrite("left", "active-1", "cActive", viaTrash = false, resolution = None), true)
        // Age everything: runs and the resolved tombstone fall behind the cutoff.
        sql"UPDATE sync_run SET started_at = TIMESTAMP WITH TIME ZONE '2020-01-01 00:00:00+00'".update.run()
        sql"UPDATE tombstone SET resolved_at = TIMESTAMP WITH TIME ZONE '2020-01-01 00:00:00+00' WHERE resolved_at IS NOT NULL".update.run()

      val (prunedRuns, prunedTombstones) = transact(db.xa)(pruneAuditData(90))

      // The old run is pruned, the baseline run survives despite its age.
      assertEquals(prunedRuns, 1)
      val remainingRuns = connect(db.xa)(sql"SELECT id FROM sync_run".query[Long].run())
      assertEquals(remainingRuns, Vector(baselineRun))
      // Baseline observations survive.
      assertEquals(connect(db.xa)(loadRunObservations(baselineRun, "left")).size, 1)
      // The resolved tombstone is pruned; the active one is untouched.
      assertEquals(prunedTombstones, 1)
      assertEquals(connect(db.xa)(selectActiveTombstones(pair.id)).map(_.checksum), Vector("cActive"))
      // deletion_log survives with run_id nulled; uploaded_asset survives.
      val (dlCount, dlRunId) = connect(db.xa):
        (sql"SELECT COUNT(*) FROM deletion_log".query[Long].run().head,
         sql"SELECT run_id FROM deletion_log".query[Option[Long]].run().head)
      assertEquals(dlCount, 1L)
      assertEquals(dlRunId, None)
      assert(connect(db.xa)(uploadedAssetExists(rightPeer.id, "x1")))
    }
  }

  test("annotation discovery on H2: stamping, groups, membership, pair lifecycle") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val api = FakeImmichApi()
      api.setAlbumList(leftPeer.baseUrl, Seq(AlbumSummary("album-a", "A", "")))
      api.setAlbumList(rightPeer.baseUrl, Seq(AlbumSummary("album-b", "B", "")))

      // Run 1: both albums get stamped with fresh unique tokens; no pairs yet.
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      assertEquals(api.descriptionUpdates.size, 2)
      assertEquals(connect(db.xa)(loadAllPairs()).size, 0)
      assertEquals(connect(db.xa)(countSyncAlbums()), 2L)

      // Run 2: albums are now annotated with singleton tokens; still no pairs, no re-stamps.
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      assertEquals(api.descriptionUpdates.size, 2)
      assertEquals(connect(db.xa)(loadAllPairs()).size, 0)
      assertEquals(connect(db.xa)(countSyncGroups()), 2L)

      // Run 3: the user overrides B's token to match A's -> one pair, one shared group.
      val tokenA = parseSyncAnnotations(api.listAlbums(ImmichServer(leftPeer.baseUrl, "k")).head.description).annotations.head.group
      api.setAlbumList(rightPeer.baseUrl, Seq(AlbumSummary("album-b", "B", s"[sync $tokenA]")))
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      val pairs3 = connect(db.xa)(loadAllPairs())
      assertEquals(pairs3.size, 1)
      assert(pairs3.head.enabled)
      assert(pairs3.head.propagateDeletes) // deletes=on is the default

      // Run 4: idempotent — same single row, membership stable.
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      assertEquals(connect(db.xa)(loadAllPairs()).size, 1)
      assertEquals(connect(db.xa)(countSyncAlbums()), 2L)

      // Run 5: B regroups -> the pair is disabled, never deleted or duplicated.
      api.setAlbumList(rightPeer.baseUrl, Seq(AlbumSummary("album-b", "B", "[sync elsewhere-group]")))
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      val pairs5 = connect(db.xa)(loadAllPairs())
      assertEquals(pairs5.size, 1)
      assert(!pairs5.head.enabled)

      // Run 6: back to the shared token -> the same row is re-enabled additively.
      api.setAlbumList(rightPeer.baseUrl, Seq(AlbumSummary("album-b", "B", s"[sync $tokenA]")))
      runAnnotationDiscovery(db, api, _ => "k", applyWrites = true)
      val pairs6 = connect(db.xa)(loadAllPairs())
      assertEquals(pairs6.size, 1)
      assert(pairs6.head.enabled)
      assert(pairs6.head.forceAdditive)
    }
  }

  test("rearm keys: issued on trip, consumed on use, re-trip issues a new key") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val repo = DbSyncRepository(db)
      var pair = seedAnnotationPair(db, leftPeer, rightPeer)

      // Baseline with one shared photo, then a strict breaker and a removal on the left.
      val run0 = repo.startRun(pair.id, dryRun = false)
      repo.finalizeRun(run0, pair, Seq.empty, Seq.empty,
        Seq(ObservationRow("a1", "c1", false)), Seq(ObservationRow("b1", "c1", false)), applyRun = true)
      transact(db.xa):
        sql"UPDATE album_pair SET max_removal_count = 0, force_additive = FALSE WHERE id = ${pair.id}".update.run()
      pair = connect(db.xa)(loadAllPairs()).head

      val api = FakeImmichApi()
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq.empty)
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1")))
      runSync(db, api, pair, leftPeer, rightPeer)

      val quarantined = connect(db.xa)(selectQuarantinedPairs())
      assertEquals(quarantined.size, 1)
      val key1 = quarantined.head._3.get
      assert(key1.nonEmpty)

      // Unknown key: no-op. Correct key: re-arms once, then is consumed.
      assertEquals(transact(db.xa)(rearmByKey("not-a-key")), None)
      assertEquals(transact(db.xa)(rearmByKey(key1)), Some(pair.name))
      assert(connect(db.xa)(selectQuarantinedPairs()).isEmpty)
      assert(connect(db.xa)(loadAllPairs()).head.forceAdditive)
      assertEquals(transact(db.xa)(rearmByKey(key1)), None)

      // Trip again: a fresh key is issued.
      transact(db.xa):
        sql"UPDATE album_pair SET force_additive = FALSE WHERE id = ${pair.id}".update.run()
      pair = connect(db.xa)(loadAllPairs()).head
      runSync(db, api, pair, leftPeer, rightPeer)
      val key2 = connect(db.xa)(selectQuarantinedPairs()).head._3.get
      assert(key1 != key2)
    }
  }

  test("per-peer circuit-breaker thresholds are stored and loaded") {
    withDb { db =>
      applySyncConfig(db, SyncConfig(peers = List(
        leftPeerCfg.copy(maxRemovalCount = Some(100), maxRemovalFraction = Some(0.9)),
        rightPeerCfg,
      )))
      val peers = connect(db.xa)(loadEnabledPeers())
      val left = peers.find(_.name == "left").get
      val right = peers.find(_.name == "right").get
      assertEquals(left.maxRemovalCount, Some(100))
      assertEquals(left.maxRemovalFraction, Some(0.9))
      assertEquals(right.maxRemovalCount, None)

      // Upsert clears overrides when the config drops them.
      applySyncConfig(db, SyncConfig(peers = List(leftPeerCfg, rightPeerCfg)))
      assertEquals(connect(db.xa)(loadEnabledPeers()).find(_.name == "left").get.maxRemovalCount, None)
    }
  }

  test("config application on H2: peer upsert keeps ids stable, endpoint change forces additive") {
    withDb { db =>
      val config = SyncConfig(
        peers = List(leftPeerCfg, rightPeerCfg),
        pairs = Some(List(PairConfig(
          "family",
          PairEndpointConfig("left", "album-1"),
          PairEndpointConfig("right", "album-2"),
        ))),
      )
      applySyncConfig(db, config)
      val (leftPeer, _) = (connect(db.xa)(loadEnabledPeers()).find(_.name == "left").get, ())
      val pair1 = connect(db.xa)(selectPairByName("family")).get
      assertEquals(pair1.linkSource, "manual")
      assert(pair1.propagateDeletes) // deletes defaults to true

      // Hostname change: same peer id afterwards.
      applySyncConfig(db, config.copy(peers = List(leftPeerCfg.copy(baseUrl = "http://left-new.local"), rightPeerCfg)))
      val leftAfter = connect(db.xa)(loadEnabledPeers()).find(_.name == "left").get
      assertEquals(leftAfter.id, leftPeer.id)
      assertEquals(leftAfter.baseUrl, "http://left-new.local")

      // Clear force_additive, then change an endpoint: it must come back.
      transact(db.xa)(updatePairForceAdditive(pair1.id, forceAdditive = false))
      applySyncConfig(db, config.copy(pairs = Some(List(PairConfig(
        "family",
        PairEndpointConfig("left", "album-OTHER"),
        PairEndpointConfig("right", "album-2"),
      )))))
      val pair2 = connect(db.xa)(selectPairByName("family")).get
      assertEquals(pair2.leftAlbumId, "album-OTHER")
      assert(pair2.forceAdditive)
    }
  }

  test("dry-run discovery writes nothing: no stamps, no pairs, no membership") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val api = FakeImmichApi()
      api.setAlbumList(leftPeer.baseUrl, Seq(AlbumSummary("album-a", "A", "")))
      api.setAlbumList(rightPeer.baseUrl, Seq(AlbumSummary("album-b", "B", "[sync grp-x]")))

      runAnnotationDiscovery(db, api, _ => "k", applyWrites = false)

      assert(api.descriptionUpdates.isEmpty)
      assertEquals(connect(db.xa)(loadAllPairs()).size, 0)
      assertEquals(connect(db.xa)(countSyncAlbums()), 0L)
      assertEquals(connect(db.xa)(countSyncGroups()), 0L)
    }
  }

  test("dry-run cycles on H2 never prune the baseline observations") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      var pair = seedAnnotationPair(db, leftPeer, rightPeer)
      val api = FakeImmichApi()
      api.setAlbumAssets(leftPeer.baseUrl, "album-left", Seq(mkAsset("a1", "c1")))
      api.setAlbumAssets(rightPeer.baseUrl, "album-right", Seq(mkAsset("b1", "c1")))
      val retention = RetentionConfig(observationKeepRuns = 2, auditRetentionDays = 90)

      runSync(db, api, pair, leftPeer, rightPeer, retention = retention)
      val baseline = connect(db.xa)(previousBaselineRunId(pair.id)).get
      pair = connect(db.xa)(loadAllPairs()).head

      (1 to 4).foreach { _ =>
        executePairSyncWith(
          pair = pair, leftPeer = leftPeer, rightPeer = rightPeer,
          applyWrites = false, repo = DbSyncRepository(db), api = api,
          resolveApiKey = _ => "dummy-key", retention = retention,
        )
      }

      assertEquals(connect(db.xa)(previousBaselineRunId(pair.id)), Some(baseline))
      assertEquals(connect(db.xa)(loadRunObservations(baseline, "left")).size, 1)
    }
  }

  test("peer names match case-insensitively: renaming case keeps the same row") {
    withDb { db =>
      applySyncConfig(db, SyncConfig(peers = List(leftPeerCfg, rightPeerCfg)))
      val originalId = connect(db.xa)(loadEnabledPeers()).find(_.name == "left").get.id

      applySyncConfig(db, SyncConfig(peers = List(leftPeerCfg.copy(name = "LEFT"), rightPeerCfg)))
      val peers = connect(db.xa)(loadAllPeers())
      assertEquals(peers.size, 2)
      val renamed = peers.find(_.name == "LEFT").get
      assertEquals(renamed.id, originalId)
    }
  }

  test("orphan cleanup end-to-end: trashes only true orphans, logs, respects dry-run and quarantine") {
    withDb { db =>
      applySyncConfig(db, SyncConfig(peers = List(leftPeerCfg.copy(cleanupOrphans = Some(true)), rightPeerCfg)))
      val peers = connect(db.xa)(loadEnabledPeers())
      val peer = peers.find(_.name == "left").get
      assert(peer.cleanupOrphans)
      val pair = seedAnnotationPair(db, peer, peers.find(_.name == "right").get)

      val api = FakeImmichApi()
      // One visible album containing a live photo still that references a video.
      val still = mkAsset("still-1", "chk-still").copy(livePhotoVideoId = Some("video-1"))
      api.setAlbumList(peer.baseUrl, Seq(AlbumSummary("album-left", "L", "[sync grp]")))
      api.setAlbumAssets(peer.baseUrl, "album-left", Seq(still))

      // Provenance: the still and its video (both protected), a favorited orphan
      // (protected), and a true orphan.
      val repo = DbSyncRepository(db)
      val seedRun = repo.startRun(pair.id, dryRun = false)
      Seq("still-1" -> "chk-still", "video-1" -> "chk-video", "fav-1" -> "chk-fav", "orphan-1" -> "chk-orphan")
        .foreach((id, chk) => repo.recordUploadedAsset(seedRun, pair.id, peer.id, id, chk))
      repo.completeRun(seedRun, "success", None)
      transact(db.xa):
        sql"UPDATE uploaded_asset SET created_at = TIMESTAMP WITH TIME ZONE '2020-01-01 00:00:00+00'".update.run()
      api.setAssetInfo(mkAsset("fav-1", "chk-fav").copy(isFavorite = true))
      api.setAssetInfo(mkAsset("orphan-1", "chk-orphan"))

      val cleanup = CleanupConfig(afterDays = 1, maxPerPass = 0)

      // Dry run: full preview, zero writes.
      runOrphanCleanup(db, api, peers, _ => "k", applyWrites = false, cleanup)
      assert(api.trashCalls.isEmpty)
      assertEquals(connect(db.xa)(sql"SELECT COUNT(*) FROM deletion_log".query[Long].run().head), 0L)

      // Apply: only the true orphan is trashed and logged.
      runOrphanCleanup(db, api, peers, _ => "k", applyWrites = true, cleanup)
      assertEquals(api.trashCalls.map(_._2).flatten.toList, List("orphan-1"))
      val logged = connect(db.xa):
        sql"SELECT asset_id, action, run_id FROM deletion_log".query[(String, String, Option[Long])].run()
      assertEquals(logged, Vector(("orphan-1", "cleanup_trash", None)))

      // Quarantined pair on the peer freezes cleanup entirely.
      transact(db.xa):
        sql"UPDATE album_pair SET quarantined_at = now() WHERE id = ${pair.id}".update.run()
      runOrphanCleanup(db, api, peers, _ => "k", applyWrites = true, cleanup)
      assertEquals(api.trashCalls.size, 1) // unchanged
    }
  }

  test("abandoned running runs are marked aborted at startup and become prunable") {
    withDb { db =>
      val (leftPeer, rightPeer) = seedPeers(db)
      val pair = seedAnnotationPair(db, leftPeer, rightPeer)
      val repo = DbSyncRepository(db)
      repo.startRun(pair.id, dryRun = false) // never finished: simulated crash

      val marked = transact(db.xa)(markAbandonedRuns())
      assertEquals(marked, 1)
      val statuses = connect(db.xa)(sql"SELECT status FROM sync_run".query[String].run())
      assertEquals(statuses, Vector("aborted"))
    }
  }
