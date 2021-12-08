package spark.jobs

import spark.jobs.adapter.{S3ClientWrapper, SparkWrapper}
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.BusinessProcessor
import spark.jobs.service.DataLoader
import spark.jobs.storage.FileRepository
import zio._
import zio.magic._

object Main extends App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      processor <- ZIO.service[BusinessProcessor]
      _         <- processor.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        S3ClientWrapper.live,
        FileRepository.live,
        SparkWrapper.live,
        BusinessProcessor.live,
        DataLoader.live,
        ZEnv.live
      )
      .exitCode
}
