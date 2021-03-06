ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

val ZioVersion       = "1.0.13"
val ZioHttpVersion   = "1.0.0.0-RC18"
val ZioConfigVersion = "1.0.10"
val LogStageVersion  = "1.0.8"
val SparkVersion     = "3.2.0"
val HadoopVersion    = "3.3.1"
val AwsSdkVersion    = "1.12.124"

val `object-storage-shared` = (project in file("object-storage-shared"))
  .settings(
    name := "object-storage-shared",
    libraryDependencies ++= Seq(
      "dev.zio"        %% "zio"                      % ZioVersion,
      "dev.zio"        %% "zio-streams"              % ZioVersion,
      "com.amazonaws"   % "aws-java-sdk-s3"          % AwsSdkVersion,
      "io.7mind.izumi" %% "logstage-core"            % LogStageVersion,
      "io.7mind.izumi" %% "logstage-adapter-slf4j"   % LogStageVersion,
      "io.7mind.izumi" %% "logstage-rendering-circe" % LogStageVersion
    )
  )

val `spark-jobs` = (project in file("spark-jobs"))
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "dev.zio"              %% "zio"                      % ZioVersion,
      "io.github.kitlangton" %% "zio-magic"                % "0.3.11",
      "dev.zio"              %% "zio-config-magnolia"      % ZioConfigVersion,
      "dev.zio"              %% "zio-config-typesafe"      % ZioConfigVersion,
      "io.7mind.izumi"       %% "logstage-core"            % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-adapter-slf4j"   % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-rendering-circe" % LogStageVersion,
      "org.apache.spark"     %% "spark-core"               % SparkVersion,
      "org.apache.spark"     %% "spark-sql"                % SparkVersion,
      "org.apache.spark"     %% "spark-mllib"              % SparkVersion,
      "org.apache.hadoop"     % "hadoop-common"            % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-aws"               % HadoopVersion,
      "org.apache.hadoop"     % "hadoop-client"            % HadoopVersion
    )
  )
  .dependsOn(`object-storage-shared`)

val `s3-loader` = (project in file("s3-loader"))
  .settings(
    name := "s3-loader",
    libraryDependencies ++= Seq(
      "dev.zio"              %% "zio"                      % ZioVersion,
      "dev.zio"              %% "zio-streams"              % ZioVersion,
      "io.github.kitlangton" %% "zio-magic"                % "0.3.11",
      "dev.zio"              %% "zio-config-magnolia"      % ZioConfigVersion,
      "dev.zio"              %% "zio-config-typesafe"      % ZioConfigVersion,
      "io.7mind.izumi"       %% "logstage-core"            % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-adapter-slf4j"   % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-rendering-circe" % LogStageVersion
    )
  )
  .dependsOn(`object-storage-shared`)

val `query-service` = (project in file("query-service"))
  .settings(
    name := "query-service",
    libraryDependencies ++= Seq(
      "dev.zio"              %% "zio"                      % ZioVersion,
      "dev.zio"              %% "zio-streams"              % ZioVersion,
      "io.d11"               %% "zhttp"                    % ZioHttpVersion,
      "io.github.kitlangton" %% "zio-magic"                % "0.3.11",
      "dev.zio"              %% "zio-config-magnolia"      % ZioConfigVersion,
      "dev.zio"              %% "zio-config-typesafe"      % ZioConfigVersion,
      "io.7mind.izumi"       %% "logstage-core"            % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-adapter-slf4j"   % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-rendering-circe" % LogStageVersion
    )
  )
  .dependsOn(`object-storage-shared`)

val `inference-service` = (project in file("inference-service"))
  .settings(
    name := "inference-service",
    libraryDependencies ++= Seq(
      "dev.zio"              %% "zio"                      % ZioVersion,
      "dev.zio"              %% "zio-streams"              % ZioVersion,
      "io.d11"               %% "zhttp"                    % ZioHttpVersion,
      "io.github.kitlangton" %% "zio-magic"                % "0.3.11",
      "dev.zio"              %% "zio-config-magnolia"      % ZioConfigVersion,
      "dev.zio"              %% "zio-config-typesafe"      % ZioConfigVersion,
      "io.7mind.izumi"       %% "logstage-core"            % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-adapter-slf4j"   % LogStageVersion,
      "io.7mind.izumi"       %% "logstage-rendering-circe" % LogStageVersion
    )
  )
  .dependsOn(`object-storage-shared`)
