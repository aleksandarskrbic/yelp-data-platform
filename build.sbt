ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val `spark-jobs` = (project in file("spark-jobs"))
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.1",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
      "org.apache.hadoop" % "hadoop-client" % "3.3.1",
    )
  )

lazy val `s3-loader` = (project in file("s3-loader"))
  .settings(
    name := "s3-loader",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.12",
      "io.github.kitlangton" %% "zio-magic" % "0.3.11",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.124",
      "dev.zio" %% "zio-test" % "1.0.12" % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
