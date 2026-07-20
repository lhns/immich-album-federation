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
      "com.lihaoyi" %% "ujson" % "4.3.2",
      "com.lihaoyi" %% "upickle" % "4.3.2",
      "com.augustnagro" %% "magnum" % "1.3.1",
      "org.postgresql" % "postgresql" % "42.7.5",
      "com.zaxxer" % "HikariCP" % "7.0.2",
      "org.flywaydb" % "flyway-core" % "11.3.4",
      "org.flywaydb" % "flyway-database-postgresql" % "11.3.4",
      "org.virtuslab" %% "scala-yaml" % "0.3.0",
      "com.softwaremill.ox" %% "core" % "1.0.5",
      "org.scalameta" %% "munit" % "1.0.2" % Test,
      "com.h2database" % "h2" % "2.3.232" % Test,
    ),
    assembly / mainClass := Some("immichsync.main"),
    assembly / assemblyJarName := "app.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")     => MergeStrategy.discard
      case PathList("META-INF", xs @ _*)           => MergeStrategy.first
      case "module-info.class"                     => MergeStrategy.discard
      case x                                       => MergeStrategy.first
    },
  )
