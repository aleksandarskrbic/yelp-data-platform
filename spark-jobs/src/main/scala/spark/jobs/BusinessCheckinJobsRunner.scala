package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.BusinessCheckinJobs

object BusinessCheckinJobsRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      businessCheckinJobs <- ZIO.service[BusinessCheckinJobs]
      _                   <- businessCheckinJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        BusinessCheckinJobs.live,
        ZEnv.live
      )
      .exitCode
}
