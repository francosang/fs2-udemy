ThisBuild / scalaVersion := "3.4.1"
ThisBuild / organization := "com.jfranco.fs2"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "fs2 - udemy"
  )
  .settings(
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.2.7",
    libraryDependencies += "co.fs2" %% "fs2-io" % "3.2.7"
  )
