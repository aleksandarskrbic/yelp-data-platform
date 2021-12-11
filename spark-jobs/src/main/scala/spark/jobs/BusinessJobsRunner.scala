package spark.jobs

import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.BusinessJobs
import spark.jobs.storage.DataSource
import zio._
import zio.magic._

object BusinessJobsRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      businessJobs <- ZIO.service[BusinessJobs]
      _            <- businessJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        BusinessJobs.live,
        ZEnv.live
      )
      .exitCode
}