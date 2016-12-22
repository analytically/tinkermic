import ReleaseTransformations._
import sbt.Keys._
import sbt.Tests
import sbtassembly.AssemblyPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport._

lazy val commonSettings = Seq(
  organization := "io.tinkermic",
  homepage := Some(url("https://github.com/analytically/tinkermic")),
  scalaVersion := "2.11.8"
)

val tinkerpopVersion = "3.2.3"

lazy val tinkermic = (project in file(".")).
  disablePlugins(sbtassembly.AssemblyPlugin).
  settings(commonSettings: _*).
  aggregate(gremlin, benchmark)

lazy val gremlin = (project in file("tinkermic-gremlin")).
  enablePlugins(BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(
    name := "tinkermic-gremlin",
    buildInfoPackage := "com.tinkermic.gremlin",
    buildInfoKeys ++= Seq[BuildInfoKey](
      resolvers,
      libraryDependencies,
      BuildInfoKey.action("gitRevision") {
        ("git rev-parse --short HEAD" !!).trim
      }
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoOptions += BuildInfoOption.ToJson,
    assemblyJarName in assembly := "tinkermic-gremlin.jar",
    assemblyMergeStrategy in assembly := {
      case PathList(ps@_*) if ps.last == "HornetQUtilBundle_$bundle.class" => MergeStrategy.first // datomic
      case PathList(ps@_*) if ps.last == "groovy-release-info.properties" => MergeStrategy.first // tinkerpop
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    publishMavenStyle := true,
    crossPaths := false,
    javacOptions in (Compile, compile) ++= Seq(
      "-source", "1.8",
      "-target", "1.8",
      "-encoding", "UTF-8",
      "-Xlint:unchecked",
      "-Xlint:deprecation"
    ),
    javacOptions in (Compile,doc) ++= Seq("-notimestamp", "-link", "http://tinkerpop.apache.org/javadocs/current/full/", "-link", "https://docs.oracle.com/javase/8/docs/api/"),
    javaOptions in Test += "-Ddatomic.objectCacheMax=256m",
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-a"),
    fork := true,
    parallelExecution in Test := false,
    resolvers += "my.datomic.com" at "https://my.datomic.com/repo",
    resolvers += "jitpack" at "https://jitpack.io",
    resolvers += "clojars" at "https://clojars.org/repo",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.22",
      "org.apache.tinkerpop" % "gremlin-core" % tinkerpopVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.tinkerpop" % "gremlin-groovy" % tinkerpopVersion % Provided exclude("org.slf4j", "slf4j-log4j12"),
      "com.datomic" % "datomic-free" % "0.9.5544" exclude("org.slf4j", "slf4j-nop") exclude("org.slf4j", "log4j-over-slf4j"),
      "org.threeten" % "threeten-extra" % "1.0",
      "org.apache.tinkerpop" % "gremlin-test" % tinkerpopVersion % Test exclude("org.slf4j", "slf4j-log4j12"),
      "ch.qos.logback" % "logback-classic" % "1.1.8" % Test,
      "junit" % "junit" % "4.12" % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test
    ),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

lazy val benchmark = (project in file("tinkermic-gremlin-benchmark")).
  settings(commonSettings: _*).
  settings(
    name := "tinkermic-gremlin-benchmark",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.7",
      "org.apache.tinkerpop" % "gremlin-test" % tinkerpopVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.tinkerpop" % "neo4j-gremlin" % tinkerpopVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.neo4j" % "neo4j-tinkerpop-api-impl" % "0.4-3.0.3",
      "org.apache.tinkerpop" % "tinkergraph-gremlin" % tinkerpopVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.openjdk.jmh" % "jmh-core" % "1.15",
      "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.15"
    ),
    crossPaths := false,
    assemblyJarName in assembly := "tinkermic-gremlin-benchmark.jar",
    assemblyMergeStrategy in assembly := {
      case PathList(ps@_*) if ps.last == "HornetQUtilBundle_$bundle.class" => MergeStrategy.first // datomic
      case PathList(ps@_*) if ps.last == "groovy-release-info.properties" => MergeStrategy.first // tinkerpop
      case PathList("org", "hamcrest", xs@_*) => MergeStrategy.last
      case PathList(ps@_*) if ps.last == "LICENSES.txt" => MergeStrategy.first // neo4j
      case PathList(ps@_*) if ps.last == "modules.properties" => MergeStrategy.first // neo4j
      case PathList(ps@_*) if ps.last == "pom.xml" => MergeStrategy.first // neo4j
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    publishArtifact := false,
    mainClass in assembly := Some("com.tinkermic.benchmark.Main")
  ).dependsOn(gremlin)
