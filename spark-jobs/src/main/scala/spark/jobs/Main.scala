package spark.jobs

import spark.jobs.adapter.{S3ClientWrapper, SparkWrapper}
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.JobsManager
import spark.jobs.storage.{DataSource, FileRepository}
import zio._
import zio.magic._

object Main extends App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      processor <- ZIO.service[JobsManager]
      _         <- processor.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        S3ClientWrapper.live,
        FileRepository.live,
        SparkWrapper.live,
        JobsManager.live,
        DataSource.live,
        ZEnv.live
      )
      .exitCode
}
