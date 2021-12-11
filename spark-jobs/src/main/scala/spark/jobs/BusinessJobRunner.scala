package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.processor.BusinessJob
import spark.jobs.common.{AppConfig, Logging}

object BusinessJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      businessJobs <- ZIO.service[BusinessJob]
      _            <- businessJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        BusinessJob.live,
        ZEnv.live
      )
      .exitCode
}
