package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.processor.ReviewClassifierJob
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.adapter.spark.SparkWrapper

object ReviewClassifierJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      reviewClassifierJob <- ZIO.service[ReviewClassifierJob]
      _                   <- reviewClassifierJob.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        ReviewClassifierJob.live,
        ZEnv.live
      )
      .exitCode
}
