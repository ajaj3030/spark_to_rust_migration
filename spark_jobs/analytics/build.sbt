// Batch analytics jobs over the enriched telemetry Delta table.
// Built as one jar and run with spark-submit --class telemetry.analytics.<Job>.

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "telemetry"

lazy val root = (project in file("."))
  .settings(
    name := "telemetry-analytics",
    version := "1.0.0",
    // Supplied by the Spark runtime (Spark 3.5.1 ships snakeyaml 2.0)
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
      "org.yaml" % "snakeyaml" % "2.0" % Provided,
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xfatal-warnings"),
    javacOptions ++= Seq("--release", "11"),
  )
