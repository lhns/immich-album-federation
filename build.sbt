ThisBuild / scalaVersion := "3.7.3"
ThisBuild / organization := "de.lhns"

lazy val root = (project in file("."))
  .settings(
    name := "immich-album-federation",
    scalacOptions ++= Seq("-deprecation", "-feature", "-release", "21"),
    javacOptions ++= Seq("--release", "21"),
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client4" %% "core" % "4.0.26",
      "com.softwaremill.sttp.client4" %% "upickle" % "4.0.26",
      "com.lihaoyi" %% "ujson" % "4.4.3",
      "com.lihaoyi" %% "upickle" % "4.4.3",
      "com.augustnagro" %% "magnum" % "1.3.1",
      "org.postgresql" % "postgresql" % "42.7.13",
      "com.zaxxer" % "HikariCP" % "7.1.0",
      "org.flywaydb" % "flyway-core" % "13.0.0",
      "org.flywaydb" % "flyway-database-postgresql" % "13.0.0",
      "org.virtuslab" %% "scala-yaml" % "0.3.2",
      "com.softwaremill.ox" %% "core" % "1.0.5",
      "ch.qos.logback" % "logback-classic" % "1.5.38" % Runtime,
      "org.scalameta" %% "munit" % "1.3.4" % Test,
      "com.h2database" % "h2" % "2.4.240" % Test,
    ),
    assembly / mainClass := Some("immichsync.main"),
    assembly / assemblyJarName := "app.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")           => MergeStrategy.discard
      // ServiceLoader registrations MUST be merged, not picked: flyway-core and
      // flyway-database-postgresql both ship the same Plugin service file, and
      // "first" silently drops the PostgreSQL support from the fat jar.
      case PathList("META-INF", "services", _*)          => MergeStrategy.filterDistinctLines
      case PathList("META-INF", _*)                      => MergeStrategy.first
      case "module-info.class"                           => MergeStrategy.discard
      case _                                             => MergeStrategy.first
    },
  )
