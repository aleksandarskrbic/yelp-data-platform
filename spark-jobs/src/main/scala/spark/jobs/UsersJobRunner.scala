package spark.jobs

import zio._
import zio.magic._
import spark.jobs.processor.UserJob
import spark.jobs.storage.DataSource
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.adapter.spark.SparkWrapper

object UsersJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      userJob <- ZIO.service[UserJob]
      _       <- userJob.start
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
