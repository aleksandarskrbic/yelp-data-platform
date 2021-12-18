package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.TrendingBusinessJob
import `object`.storage.shared.s3.{S3Client, S3ClientWrapper}

object TrendingBusinessJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      trendingBusinessJob <- ZIO.service[TrendingBusinessJob]
      _                   <- trendingBusinessJob.start
    } yield ())
      .inject(
        AppConfig.live,
        AppConfig.subLayers,
        Logging.live,
        S3ClientWrapper.live,
        S3Client.live,
        SparkWrapper.live,
        DataSource.live,
        TrendingBusinessJob.live,
        ZEnv.live
      )
      .exitCode
}
