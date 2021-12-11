package spark.jobs

import zio._
import zio.magic._
import spark.jobs.processor.UserJob
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}

object UsersJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      userJobs <- ZIO.service[UserJob]
      _        <- userJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        UserJob.live,
        ZEnv.live
      )
      .exitCode
}
