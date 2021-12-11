package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.TrendingBusinessJob

object TrendingBusinessJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      trendingBusinessJob <- ZIO.service[TrendingBusinessJob]
      _                   <- trendingBusinessJob.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        TrendingBusinessJob.live,
        ZEnv.live
      )
      .exitCode
}
