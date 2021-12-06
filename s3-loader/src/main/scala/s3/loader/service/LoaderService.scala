package s3.loader.service

import zio._
import logstage.LogZIO.log
import s3.loader.common.AppConfig
import s3.loader.storage.S3Client

import java.io.File

final class LoaderService(appConfig: AppConfig, uploadService: UploadService) {
  def start = ZIO.effect(createRootDirectory).flatMap { rootDirectory =>
    ZIO
      .effect(rootDirectory.list())
      .onError(error => log.error(s"Unable to list files in directory. $error").as(List.empty))
      .flatMap(files => ZIO.succeed(files))
  }

  private def createRootDirectory: File =
    new File(appConfig.upload.directory)
}

object LoaderService {
  lazy val live = (for {
    appConfig     <- ZIO.service[AppConfig]
    uploadService <- ZIO.service[UploadService]
  } yield new LoaderService(appConfig, uploadService)).toLayer
}
