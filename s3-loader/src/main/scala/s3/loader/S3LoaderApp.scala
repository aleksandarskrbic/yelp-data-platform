package s3.loader

import zio._
import zio.magic._

import logstage.LogZIO.log
import s3.loader.service.{LoaderService, UploadService}
import s3.loader.common.{AppConfig, Logging}
import s3.loader.storage.{S3Client, S3ClientWrapper}

object S3LoaderApp extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _             <- log.info("Starting s3-loader")
      loaderService <- ZIO.service[LoaderService]
      files         <- loaderService.start
      _             <- ZIO.effect(println(files.mkString("Array(", ", ", ")")))
    } yield ())
      .inject(AppConfig.live, Logging.live, S3ClientWrapper.live, S3Client.live, UploadService.live, LoaderService.live)
      .exitCode
}
