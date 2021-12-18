package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.BusinessCheckinJob
import spark.jobs.adapter.spark.SparkWrapper

object BusinessCheckinJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      businessCheckinJob <- ZIO.service[BusinessCheckinJob]
      _                  <- businessCheckinJob.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        BusinessCheckinJob.live,
        ZEnv.live
      )
      .exitCode
}
