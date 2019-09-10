name := "SparkCSTemplate"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
val catsVersion = "1.6.0"

lazy val projectDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.github.pureconfig" %% "pureconfig" % "0.9.2",
  "com.github.nscala-time" %% "nscala-time" % "2.22.0",
  "org.typelevel" %% "cats-core" % catsVersion withSources(),
  "org.typelevel" %% "cats-kernel" % catsVersion withSources(),
  "org.typelevel" %% "cats-macros" % catsVersion withSources(),
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

libraryDependencies ++= projectDeps

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

scalacOptions ++= Seq("-Ypartial-unification", "-unchecked", "-deprecation", "-feature", "-target:jvm-1.8", "-Ywarn-unused-import")
scalacOptions in(Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits")

test in assembly := {}

fork in Test := true
javaOptions ++= Seq("-Xms2g", "-Xmx3g", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := true


resolvers += "jitpack" at "https://jitpack.io"