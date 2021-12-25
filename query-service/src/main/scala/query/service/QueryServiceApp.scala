package query.service

import zio._
import zio.magic._
import zio.stream._
import logstage.LogZIO
import logstage.LogZIO.log
import query.service.loader.FileLoader
import query.service.common.{AppConfig, Logging}
import `object`.storage.shared.s3.{S3Client, S3ClientWrapper}

object QueryServiceApp extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      fileLoader <- ZIO.service[FileLoader]
      c          <- fileLoader.businessCheckinDetails.runCollect
    } yield ())
      .inject(
        AppConfig.live,
        AppConfig.subLayers,
        Logging.live,
        S3ClientWrapper.live,
        S3Client.live,
        FileLoader.live,
        ZEnv.live
      )
      .exitCode
}
