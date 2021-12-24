package query.service

import `object`.storage.shared.s3.{S3Client, S3ClientWrapper}
import logstage.LogZIO
import logstage.LogZIO.log
import query.service.common.{AppConfig, Logging}
import query.service.loader.FileLoader
import zio._
import zio.magic._
import zio.stream._

object QueryServiceApp extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      fileLoader <- ZIO.service[FileLoader]
      _          <- fileLoader.businessByIsOpen().foreach(e => ZIO.effect(println(e)))
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
