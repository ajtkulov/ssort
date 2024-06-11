name := "ssort"

version := "0.1"

run / javaOptions += "-Xmx16G"

javaOptions in(Test, run) += "-Xmx16G"

parallelExecution in Test := false

fork := true

lazy val commonSettings = Seq(
  organization := "ajtkulov.github.com",
  scalaVersion := "2.13.6",
  sources in(Compile, doc) := Seq.empty,
)

parallelExecution in Test := false


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value, // Add scala-reflect dependency
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

libraryDependencies += "org.rogach" %% "scallop" % "5.1.0"