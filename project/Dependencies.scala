import sbt._

object Dependencies {
  val scalaLanguageVersion = "2.13.2"
  val json4sVersion = "3.6.8"

  lazy val json4sNative = "org.json4s" %% "json4s-native" % json4sVersion
  lazy val json4sCore = "org.json4s" %% "json4s-core" % json4sVersion

  lazy val coreDependencies: Seq[ModuleID] = Seq(
    "org.scala-lang" % "scala-library" % scalaLanguageVersion,
    "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0",
    "org.scalatest" %% "scalatest" % "3.1.2" % Test
  )

  lazy val datageneratorDependencies: Seq[ModuleID] = Seq(
    "org.tukaani" % "xz" % "1.8",
    json4sNative,
    json4sCore
  )

  lazy val samplerDependencies: Seq[ModuleID] = Seq()

  lazy val sparkDependencies: Seq[ModuleID] = Seq()

  lazy val decisionCoreDependencies: Seq[ModuleID] = Seq()

  lazy val benchmarkDependencies: Seq[ModuleID] = Seq(
    json4sNative,
    json4sCore
  )

  lazy val vizualizationDependencies: Seq[ModuleID] = Seq(
    "org.knowm.xchart" % "xchart" % "3.6.3",
    "de.erichseifert.vectorgraphics2d" % "VectorGraphics2D" % "0.13"
  )
}