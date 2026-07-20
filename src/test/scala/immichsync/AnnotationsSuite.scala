package immichsync

class AnnotationsSuite extends munit.FunSuite:
  private val peerOne = SyncPeer(1L, "one", "http://one.local", enabled = true)
  private val peerTwo = SyncPeer(2L, "two", "http://two.local", enabled = true)
  private val peers = Seq(peerOne, peerTwo)

  private def album(id: String, name: String, description: String, ownerEmail: Option[String] = None): AlbumSummary =
    AlbumSummary(id, name, description, ownerEmail)

  private def tokenGen(prefix: String): () => String =
    var counter = 0
    () => { counter += 1; s"$prefix$counter" }

  // -------------------------------------------------------------------------
  // parseSyncAnnotations
  // -------------------------------------------------------------------------

  test("parser: plain annotation without options") {
    val parsed = parseSyncAnnotations("Holiday photos [sync family] shared with everyone")
    assertEquals(parsed.annotations, Seq(SyncAnnotation("family", None, None)))
    assert(parsed.warnings.isEmpty)
  }

  test("parser: options are parsed") {
    val parsed = parseSyncAnnotations("[sync family deletes=off direction=push]")
    assertEquals(parsed.annotations, Seq(SyncAnnotation("family", Some(false), Some("push"))))
  }

  test("parser: peers= and owners= allow-lists, lowercased, combinable") {
    val parsed = parseSyncAnnotations("[sync family peers=Beta,GAMMA owners=Alice@Example.org deletes=off]")
    assertEquals(
      parsed.annotations,
      Seq(SyncAnnotation("family", Some(false), None, Some(Set("beta", "gamma")), Some(Set("alice@example.org")))),
    )
    // Empty lists are invalid, not "allow nothing silently".
    assert(parseSyncAnnotations("[sync family peers=,]").annotations.isEmpty)
    assertEquals(parseSyncAnnotations("[sync family peers=,]").warnings.size, 1)
  }

  test("parser: invalid option drops the annotation with a warning") {
    val parsed = parseSyncAnnotations("[sync family deletes=maybe]")
    assert(parsed.annotations.isEmpty)
    assertEquals(parsed.warnings.size, 1)
  }

  test("parser: legacy mirror annotation warns") {
    val parsed = parseSyncAnnotations("[mirror two aaaaaaaa-1111-2222-3333-444444444444]")
    assert(parsed.annotations.isEmpty)
    assert(parsed.warnings.exists(_.contains("no longer supported")))
  }

  test("parser: group token length limits") {
    assert(parseSyncAnnotations("[sync ab]").annotations.isEmpty)
    assert(parseSyncAnnotations("[sync abc]").annotations.nonEmpty)
    assert(parseSyncAnnotations(s"[sync ${"a" * 64}]").annotations.nonEmpty)
    assert(parseSyncAnnotations(s"[sync ${"a" * 65}]").annotations.isEmpty)
  }

  test("generated tokens are valid group tokens") {
    (1 to 20).foreach { _ =>
      val token = generateGroupToken()
      assertEquals(token.length, 13)
      assertEquals(parseSyncAnnotations(s"[sync $token]").annotations, Seq(SyncAnnotation(token, None, None)))
    }
  }

  test("base32 encodes RFC 4648 lowercase without padding") {
    assertEquals(base32("foobar".getBytes("UTF-8")), "mzxw6ytboi")
    assertEquals(base32(Array.emptyByteArray), "")
  }

  // -------------------------------------------------------------------------
  // planDiscovery
  // -------------------------------------------------------------------------

  test("discovery: unannotated shared album is stamped with a fresh token") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "my vacation")),
      2L -> Seq.empty,
    ), tokenGen("tok"))

    assertEquals(plan.autoAnnotations.size, 1)
    val stamp = plan.autoAnnotations.head
    assertEquals(stamp.token, "tok1")
    assertEquals(stamp.newDescription, "my vacation\n[sync tok1]")
    assert(plan.links.isEmpty)
    // Membership row exists (group NULL until the stamp lands and is re-scanned).
    assertEquals(plan.memberships, Seq(AlbumMembership(1L, "album-a", None, None, None)))
  }

  test("discovery: stamping an empty description does not add a leading newline") {
    val plan = planDiscovery(peers, Map(1L -> Seq(album("album-a", "A", "")), 2L -> Seq.empty), tokenGen("tok"))
    assertEquals(plan.autoAnnotations.head.newDescription, "[sync tok1]")
  }

  test("discovery: same group links two albums bidirectionally with deletes on by default") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync family]")),
      2L -> Seq(album("album-b", "B", "[sync family]")),
    ))

    assertEquals(plan.links, Seq(DiscoveredLink(1L, "album-a", 2L, "album-b", "bidirectional", propagateDeletes = true)))
    assert(plan.autoAnnotations.isEmpty)
  }

  test("discovery: deletes=off on either member vetoes propagation") {
    def link(descA: String, descB: String): DiscoveredLink =
      planDiscovery(peers, Map(
        1L -> Seq(album("album-a", "A", descA)),
        2L -> Seq(album("album-b", "B", descB)),
      )).links.head

    assertEquals(link("[sync g-1]", "[sync g-1]").propagateDeletes, true)
    assertEquals(link("[sync g-1 deletes=off]", "[sync g-1]").propagateDeletes, false)
    assertEquals(link("[sync g-1]", "[sync g-1 deletes=off]").propagateDeletes, false)
    assertEquals(link("[sync g-1 deletes=on]", "[sync g-1 deletes=on]").propagateDeletes, true)
  }

  test("discovery: three-member group produces three pairwise links") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync trip]"), album("album-c", "C", "[sync trip]")),
      2L -> Seq(album("album-b", "B", "[sync trip]")),
    ))

    assertEquals(plan.links.size, 3)
    assertEquals(
      plan.links.map(l => ((l.leftPeerId, l.leftAlbumId), (l.rightPeerId, l.rightAlbumId))).toSet,
      Set(
        ((1L, "album-a"), (1L, "album-c")),
        ((1L, "album-a"), (2L, "album-b")),
        ((1L, "album-c"), (2L, "album-b")),
      ),
    )
  }

  test("discovery: push/pull directions produce one-way modes within the group") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync g-1 direction=push]")),
      2L -> Seq(album("album-b", "B", "[sync g-1 direction=pull]")),
    ))
    assertEquals(plan.links.head.mode, "left_to_right")

    val contradictory = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync g-1 direction=push]")),
      2L -> Seq(album("album-b", "B", "[sync g-1 direction=push]")),
    ))
    assert(contradictory.links.isEmpty)
    assert(contradictory.warnings.exists(_.contains("contradictory")))
  }

  test("discovery: single-member group waits for a partner") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync lonely]")),
      2L -> Seq.empty,
    ))
    assert(plan.links.isEmpty)
    assert(plan.warnings.exists(_.contains("single member")))
  }

  test("discovery: multiple annotations on one album use the first with a warning") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync first] [sync second]")),
      2L -> Seq(album("album-b", "B", "[sync first]")),
    ))
    assertEquals(plan.links.size, 1)
    assert(plan.warnings.exists(_.contains("using the first")))
  }

  test("discovery: peers= restricts which instances may join, both directions") {
    def links(descA: String, descB: String) =
      planDiscovery(peers, Map(
        1L -> Seq(album("album-a", "A", descA)),
        2L -> Seq(album("album-b", "B", descB)),
      )).links

    // Partner's peer listed: linked.
    assertEquals(links("[sync g-1 peers=two]", "[sync g-1]").size, 1)
    // Partner's peer not listed: blocked, even though the partner has no restriction.
    assertEquals(links("[sync g-1 peers=other]", "[sync g-1]").size, 0)
    // Restriction on the other side blocks just the same.
    assertEquals(links("[sync g-1]", "[sync g-1 peers=other]").size, 0)
    // Same-instance pair requires the shared peer's own name when restricted.
    val samePeer = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync g-1 peers=one]"), album("album-b", "B", "[sync g-1]")),
      2L -> Seq.empty,
    ))
    assertEquals(samePeer.links.size, 1)
  }

  test("discovery: owners= restricts by partner album owner and fails closed") {
    def plan(descA: String, ownerB: Option[String]) =
      planDiscovery(peers, Map(
        1L -> Seq(album("album-a", "A", descA, ownerEmail = Some("me@a.example"))),
        2L -> Seq(album("album-b", "B", "[sync g-1]", ownerEmail = ownerB)),
      ))

    assertEquals(plan("[sync g-1 owners=friend@b.example]", Some("Friend@B.example")).links.size, 1)
    val blocked = plan("[sync g-1 owners=friend@b.example]", Some("mallory@b.example"))
    assertEquals(blocked.links.size, 0)
    assert(blocked.warnings.exists(_.contains("does not allow owner 'mallory@b.example'")))
    // Unknown partner owner + owners= present: blocked, never silently allowed.
    val unknown = plan("[sync g-1 owners=friend@b.example]", None)
    assertEquals(unknown.links.size, 0)
    assert(unknown.warnings.exists(_.contains("owner of album-b is unknown")))
  }

  test("discovery: restrictions prune only the affected edges of a group") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync trip peers=one]"), album("album-c", "C", "[sync trip]")),
      2L -> Seq(album("album-b", "B", "[sync trip]")),
    ))
    // A only allows its own instance: A-C links, A-B is blocked, B-C links.
    assertEquals(
      plan.links.map(l => ((l.leftPeerId, l.leftAlbumId), (l.rightPeerId, l.rightAlbumId))).toSet,
      Set(((1L, "album-a"), (1L, "album-c")), ((1L, "album-c"), (2L, "album-b"))),
    )
    assert(plan.warnings.exists(_.contains("does not allow peer 'two'")))
  }

  test("discovery: group tokens are case-insensitive") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync Family]")),
      2L -> Seq(album("album-b", "B", "[sync family]")),
    ))
    assertEquals(plan.links.size, 1)
  }

  test("discovery: same-peer albums can form a group") {
    val plan = planDiscovery(peers, Map(
      1L -> Seq(album("album-a", "A", "[sync local]"), album("album-b", "B", "[sync local]")),
      2L -> Seq.empty,
    ))
    assertEquals(plan.links, Seq(DiscoveredLink(1L, "album-a", 1L, "album-b", "bidirectional", propagateDeletes = true)))
  }

  // -------------------------------------------------------------------------
  // reconcilePairs
  // -------------------------------------------------------------------------

  private val link = DiscoveredLink(1L, "album-a", 2L, "album-b", "bidirectional", propagateDeletes = true)

  private def annotationPair(
      id: Long,
      mode: String = "bidirectional",
      propagateDeletes: Boolean = true,
      enabled: Boolean = true,
      linkSource: String = "annotation",
  ): AlbumPair =
    AlbumPair(
      id = id,
      name = s"auto/one-two/$id",
      leftPeerId = 1L,
      leftAlbumId = "album-a",
      rightPeerId = 2L,
      rightAlbumId = "album-b",
      mode = mode,
      propagateDeletes = propagateDeletes,
      enabled = enabled,
      linkSource = linkSource,
    )

  test("reconcile: new link is inserted") {
    val rec = reconcilePairs(Vector.empty, Seq(link), scannedPeerIds = Set(1L, 2L))
    assertEquals(rec.inserts, Seq(link))
    assert(rec.updates.isEmpty && rec.disables.isEmpty)
  }

  test("reconcile: unchanged link needs no action") {
    val rec = reconcilePairs(Vector(annotationPair(5L)), Seq(link), scannedPeerIds = Set(1L, 2L))
    assert(rec.inserts.isEmpty && rec.updates.isEmpty && rec.disables.isEmpty)
  }

  test("reconcile: changed config updates the pair") {
    val rec = reconcilePairs(Vector(annotationPair(5L, propagateDeletes = false)), Seq(link), scannedPeerIds = Set(1L, 2L))
    assertEquals(rec.updates, Seq(PairUpdate(5L, "bidirectional", true, reenable = false)))
  }

  test("reconcile: disabled pair is re-enabled with force_additive") {
    val rec = reconcilePairs(Vector(annotationPair(5L, enabled = false)), Seq(link), scannedPeerIds = Set(1L, 2L))
    assertEquals(rec.updates, Seq(PairUpdate(5L, "bidirectional", true, reenable = true)))
  }

  test("reconcile: vanished group membership disables only annotation pairs") {
    val rec = reconcilePairs(
      Vector(annotationPair(5L), annotationPair(6L, linkSource = "manual")),
      Seq.empty,
      scannedPeerIds = Set(1L, 2L),
    )
    assertEquals(rec.disables, Seq(5L))
  }

  test("reconcile: pairs of unscanned peers are never disabled") {
    // Peer 2 is disabled or unreachable: its albums were not scanned, so the vanished
    // membership must not be interpreted as unlinking.
    val rec = reconcilePairs(Vector(annotationPair(5L)), Seq.empty, scannedPeerIds = Set(1L))
    assert(rec.disables.isEmpty)
  }

  test("reconcile: manual pair with matching endpoints is left alone") {
    val rec = reconcilePairs(
      Vector(annotationPair(6L, mode = "left_to_right", linkSource = "manual")),
      Seq(link),
      scannedPeerIds = Set(1L, 2L),
    )
    assert(rec.inserts.isEmpty && rec.updates.isEmpty && rec.disables.isEmpty)
    assert(rec.warnings.exists(_.contains("manually configured")))
  }

  test("reconcile: flipped stored endpoints still match with flipped mode") {
    val flippedPair = AlbumPair(
      id = 7L,
      name = "auto/x",
      leftPeerId = 2L,
      leftAlbumId = "album-b",
      rightPeerId = 1L,
      rightAlbumId = "album-a",
      mode = "left_to_right",
      propagateDeletes = true,
      enabled = true,
      linkSource = "annotation",
    )
    val oneWay = link.copy(mode = "left_to_right")
    val rec = reconcilePairs(Vector(flippedPair), Seq(oneWay), scannedPeerIds = Set(1L, 2L))
    assertEquals(rec.updates, Seq(PairUpdate(7L, "right_to_left", true, reenable = false)))
    assert(rec.disables.isEmpty)
  }

  // -------------------------------------------------------------------------
  // Onboarding scenario: the canonical share -> stamp -> override flow, with
  // row-count assertions after every step (no junk accumulation).
  // -------------------------------------------------------------------------

  test("onboarding scenario: share, stamp, override, idempotency, re-group") {
    val peerNames = peers.map(p => p.id -> p.name).toMap
    var pairRows = Vector.empty[AlbumPair]
    var nextPairId = 1L

    // Simulated album state per peer (what listAlbums would return).
    var albumsOne = Seq(album("album-a", "A", "vacation"))
    var albumsTwo = Seq.empty[AlbumSummary]

    def discover(gen: () => String): (DiscoveryPlan, PairReconciliation) =
      val plan = planDiscovery(peers, Map(1L -> albumsOne, 2L -> albumsTwo), gen)
      val rec = reconcilePairs(pairRows, plan.links, scannedPeerIds = Set(1L, 2L))
      // Apply reconciliation to the simulated pair table (what runAnnotationDiscovery does in the DB).
      rec.inserts.foreach { l =>
        pairRows = pairRows :+ AlbumPair(
          nextPairId, autoPairName(peerNames, l), l.leftPeerId, l.leftAlbumId, l.rightPeerId, l.rightAlbumId,
          l.mode, l.propagateDeletes, enabled = true, linkSource = "annotation",
        )
        nextPairId += 1
      }
      rec.updates.foreach { u =>
        pairRows = pairRows.map(p =>
          if (p.id == u.pairId) p.copy(mode = u.mode, propagateDeletes = u.propagateDeletes, enabled = true) else p
        )
      }
      rec.disables.foreach { id =>
        pairRows = pairRows.map(p => if (p.id == id) p.copy(enabled = false) else p)
      }
      // Apply auto-annotation stamps to the simulated album state.
      plan.autoAnnotations.foreach { stamp =>
        if (stamp.peerId == 1L) albumsOne = albumsOne.map(a => if (a.id == stamp.albumId) a.copy(description = stamp.newDescription) else a)
        else albumsTwo = albumsTwo.map(a => if (a.id == stamp.albumId) a.copy(description = stamp.newDescription) else a)
      }
      (plan, rec)

    // Run 1: album A is shared -> stamped with id1, single-member group, no pairs.
    val (plan1, rec1) = discover(tokenGen("id-a-"))
    assertEquals(plan1.autoAnnotations.size, 1)
    assertEquals(albumsOne.head.description, "vacation\n[sync id-a-1]")
    assertEquals(pairRows.size, 0)
    assert(rec1.inserts.isEmpty)

    // Run 2: album B is shared on peer two -> stamped with its own id, still no pairs.
    albumsTwo = Seq(album("album-b", "B", ""))
    val (plan2, _) = discover(tokenGen("id-b-"))
    assertEquals(plan2.autoAnnotations.size, 1)
    assertEquals(albumsTwo.head.description, "[sync id-b-1]")
    assertEquals(pairRows.size, 0)

    // Run 3: the user overrides B's token with A's -> exactly ONE pair is created.
    albumsTwo = Seq(album("album-b", "B", "[sync id-a-1]"))
    val (_, rec3) = discover(tokenGen("unused"))
    assertEquals(rec3.inserts.size, 1)
    assertEquals(pairRows.size, 1)
    val pair = pairRows.head
    assertEquals((pair.leftPeerId, pair.leftAlbumId, pair.rightPeerId, pair.rightAlbumId), (1L, "album-a", 2L, "album-b"))
    assertEquals(pair.mode, "bidirectional")
    assertEquals(pair.propagateDeletes, true)
    assert(pair.forceAdditive)
    assert(pair.enabled)

    // Runs 4-6: repeated discovery is idempotent â€” no new rows, no stamps, no churn.
    (1 to 3).foreach { _ =>
      val (planN, recN) = discover(tokenGen("unused"))
      assert(planN.autoAnnotations.isEmpty)
      assert(recN.inserts.isEmpty && recN.updates.isEmpty && recN.disables.isEmpty)
      assertEquals(pairRows.size, 1)
    }

    // Re-group: B moves to a different token -> the old pair is disabled, not deleted,
    // not duplicated; no enabled orphans remain.
    albumsTwo = Seq(album("album-b", "B", "[sync id-other]"))
    val (_, recRegroup) = discover(tokenGen("unused"))
    assertEquals(recRegroup.disables.size, 1)
    assertEquals(pairRows.size, 1)
    assert(!pairRows.head.enabled)

    // Idempotent again: the disabled pair is not re-disabled or duplicated.
    val (_, recAgain) = discover(tokenGen("unused"))
    assert(recAgain.inserts.isEmpty && recAgain.updates.isEmpty && recAgain.disables.isEmpty)

    // Back to the shared token: the SAME row is re-enabled with force_additive.
    albumsTwo = Seq(album("album-b", "B", "[sync id-a-1]"))
    val (_, recRelink) = discover(tokenGen("unused"))
    assertEquals(recRelink.updates.map(_.reenable), Seq(true))
    assertEquals(pairRows.size, 1)
    assert(pairRows.head.enabled)
  }
