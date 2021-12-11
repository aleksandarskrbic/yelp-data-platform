package spark.jobs

import zio._
import zio.magic._
import spark.jobs.processor.UserJobs
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}

object UsersJobsRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      userJobs <- ZIO.service[UserJobs]
      _        <- userJobs.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        UserJobs.live,
        ZEnv.live
      )
      .exitCode
}
