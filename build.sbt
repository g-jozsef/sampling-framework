import Dependencies._

import scala.concurrent.duration.DurationDouble
import scala.io.Source

name := "sampling"

scalaVersion in ThisBuild := scalaLanguageVersion

forceUpdatePeriod := Some(5.minutes)

lazy val commonSettings = Seq(
  organizationName := "ELTE IK",
  organization := "hu.elte.inf.sampling",
  version := {
    val vSource = Source.fromFile("version", "UTF-8")
    val v = vSource.mkString
    vSource.close()
    if (!v.matches("""^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$""")) {
      throw new RuntimeException("Invalid version format!")
    }
    v
  },

  logLevel in test := Level.Debug,

  resolvers in ThisBuild ++= Seq("Maven Central" at "https://repo1.maven.org/maven2/"),

  fork in Test := true,
  baseDirectory in Test := (baseDirectory in ThisBuild).value,
  test in assembly := {},
  assemblyOutputPath in assembly := file("bin/benchmark.jar")
)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "core",
    description := "Core, collection of utilities used in all submodules",
    libraryDependencies ++= coreDependencies,
    assemblyJarName in assembly := "core.jar"
  )

lazy val datagenerator = (project in file("datagenerator")).
  settings(commonSettings: _*).
  settings(
    name := "datagenerator",
    description := "DataGenerator, a generator for data streams with concept drift",
    libraryDependencies ++= datageneratorDependencies,
    assemblyJarName in assembly := "core.datagenerator"
  ).
  dependsOn(
    core % "test->test;compile->compile"
  )

lazy val visualization = (project in file("visualization")).
  settings(commonSettings: _*).
  settings(
    name := "visualization",
    description := "Visualization, plotter for graphs",
    libraryDependencies ++= vizualizationDependencies,
    assemblyJarName in assembly := "visualization.jar"
  ).
  dependsOn(
    core % "test->test;compile->compile",
    datagenerator % "test->test;compile->compile"
  )

lazy val sampler = (project in file("sampler")).
  settings(commonSettings: _*).
  settings(
    name := "sampler",
    description := "Sampler, collection of sampling algorithms",
    libraryDependencies ++= samplerDependencies,
    assemblyJarName in assembly := "sampler.jar"
  ).
  dependsOn(
    core % "test->test;compile->compile"
  )

lazy val decisioncore = (project in file("decisioncore")).
  settings(commonSettings: _*).
  settings(
    name := "decisioncore",
    description := "Decision Core, collection of decider strategies",
    libraryDependencies ++= decisionCoreDependencies,
    assemblyJarName in assembly := "decisioncore.jar"
  ).
  dependsOn(
    core % "test->test;compile->compile",
  )

lazy val benchmark = (project in file("benchmark")).
  settings(commonSettings: _*).
  settings(
    name := "benchmark",
    description := "Benchmark, benchmarking and visualizing sampling algorithms",
    libraryDependencies ++= benchmarkDependencies,
    mainClass in assembly := Some("hu.elte.inf.sampling.benchmark.Benchmark")
  ).
  dependsOn(
    core % "test->test;compile->compile",
    sampler % "test->test;compile->compile",
    visualization % "test->test;compile->compile;runtime->runtime",
    datagenerator % "test->test;compile->compile",
    decisioncore % "test->test;compile->compile"
  )

lazy val sampling = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("hu.elte.inf.sampling.benchmark.Benchmark")
  ).
  aggregate(core, datagenerator, sampler, visualization, benchmark).
  dependsOn(
    core % "test->test;compile->compile",
    datagenerator % "test->test;compile->compile",
    sampler % "test->test;compile->compile",
    visualization % "test->test;compile->compile;runtime->runtime",
    benchmark % "test->test;compile->compile",
    decisioncore % "test->test;compile->compile"
  )
