package spark.jobs

import zio._
import zio.magic._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.processor.ReviewJobs
import spark.jobs.common.{AppConfig, Logging}

object ReviewJobsRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      reviewJobs <- ZIO.service[ReviewJobs]
      _          <- reviewJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        ReviewJobs.live,
        ZEnv.live
      )
      .exitCode
}
