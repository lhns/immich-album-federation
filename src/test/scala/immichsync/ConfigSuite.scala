package immichsync

class ConfigSuite extends munit.FunSuite:
  private val uuidA = "aaaaaaaa-1111-2222-3333-444444444444"
  private val uuidB = "bbbbbbbb-1111-2222-3333-444444444444"

  private val validYaml =
    s"""peers:
       |  - name: source
       |    baseUrl: http://192.168.1.10:2283
       |    apiKeyEnv: IMMICH_SOURCE_API_KEY
       |  - name: target
       |    baseUrl: https://immich.example.org
       |    apiKeyEnv: IMMICH_TARGET_API_KEY
       |pairs:
       |  - name: family
       |    left: { peer: source, album: $uuidA }
       |    right: { peer: target, album: $uuidB }
       |    mode: bidirectional
       |    deletes: false
       |""".stripMargin

  test("config: parses peers and pairs") {
    val config = parseSyncConfig(validYaml).fold(e => fail(e), identity)
    assertEquals(config.peers.map(_.name), List("source", "target"))
    assertEquals(config.peers.head.apiKeyEnv, "IMMICH_SOURCE_API_KEY")
    val pair = config.allPairs.head
    assertEquals(pair.name, "family")
    assertEquals(pair.left, PairEndpointConfig("source", uuidA))
    assertEquals(pair.deletes, Some(false))
    assertEquals(pair.trashOrphans, None)
    assert(validateSyncConfig(config).isEmpty)
  }

  test("config: peers-only file is valid") {
    val config = parseSyncConfig(
      """peers:
        |  - name: source
        |    baseUrl: http://192.168.1.10:2283
        |    apiKeyEnv: IMMICH_SOURCE_API_KEY
        |""".stripMargin
    ).fold(e => fail(e), identity)
    assert(config.allPairs.isEmpty)
    assert(validateSyncConfig(config).isEmpty)
  }

  test("config: validation catches duplicates, unknown peers, bad mode and self links") {
    val config = SyncConfig(
      peers = List(
        PeerConfig("source", "http://a", "K1"),
        PeerConfig("SOURCE", "http://b", "K2"),
      ),
      pairs = Some(List(
        PairConfig("p1", PairEndpointConfig("source", uuidA), PairEndpointConfig("nowhere", uuidB)),
        PairConfig("p1", PairEndpointConfig("source", uuidA), PairEndpointConfig("source", uuidA), mode = Some("sideways")),
        PairConfig("p2", PairEndpointConfig("source", uuidA), PairEndpointConfig("source", uuidB), maxRemovalFraction = Some(1.5)),
      )),
    )
    val errors = validateSyncConfig(config)
    assert(errors.exists(_.contains("duplicate peer name")))
    assert(errors.exists(_.contains("duplicate pair name")))
    assert(errors.exists(_.contains("unknown peer 'nowhere'")))
    assert(errors.exists(_.contains("invalid mode 'sideways'")))
    assert(errors.exists(_.contains("links an album to itself")))
    assert(errors.exists(_.contains("invalid maxRemovalFraction")))
  }

  test("config: malformed yaml is a Left") {
    assert(parseSyncConfig("peers: [ {{{").isLeft)
  }

  test("interval: units, plain seconds, whitespace and empty") {
    assertEquals(parseIntervalSeconds("30s"), Some(30L))
    assertEquals(parseIntervalSeconds("15m"), Some(900L))
    assertEquals(parseIntervalSeconds("1h"), Some(3600L))
    assertEquals(parseIntervalSeconds("300"), Some(300L))
    assertEquals(parseIntervalSeconds("15 m"), Some(900L))
    assertEquals(parseIntervalSeconds(" 15M "), Some(900L))
    assertEquals(parseIntervalSeconds(""), None)
    assertEquals(parseIntervalSeconds("  "), None)
    intercept[RuntimeException](parseIntervalSeconds("15x"))
    intercept[RuntimeException](parseIntervalSeconds("m"))
  }
