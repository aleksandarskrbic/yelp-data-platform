package s3.loader.storage

import zio._
import logstage.LogZIO
import logstage.LogZIO.log
import s3.loader.config.AppConfig

final class UploadService(
  s3Client: S3Client,
  storageConfig: AppConfig.Storage,
  uploadConfig: AppConfig.Upload
) {

  def upload() = ???
}

object UploadService {
  lazy val live = (for {
    appConfig    <- ZIO.service[AppConfig]
    s3Client     <- ZIO.service[S3Client]
    storageConfig = appConfig.storage
    uploadConfig  = appConfig.upload
  } yield new UploadService(s3Client, storageConfig, uploadConfig)).toLayer
}
