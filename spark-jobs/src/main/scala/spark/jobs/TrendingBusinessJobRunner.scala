package spark.jobs

import zio._
import zio.magic._
import spark.jobs.adapter.s3.S3ClientWrapper
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.TrendingBusinessJob
import spark.jobs.storage.{DataSource, FileRepository}

object TrendingBusinessJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      trendingBusinessJob <- ZIO.service[TrendingBusinessJob]
      _                   <- trendingBusinessJob.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        S3ClientWrapper.live,
        SparkWrapper.live,
        DataSource.live,
        FileRepository.live,
        TrendingBusinessJob.live,
        ZEnv.live
      )
      .exitCode
}
