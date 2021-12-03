ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "yelp-data-platform",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.12",
      "io.github.kitlangton" %% "zio-magic" % "0.3.11",
      "dev.zio" %% "zio-test" % "1.0.12" % Test,
      "org.apache.spark" %% "spark-core" % "3.2.0"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
