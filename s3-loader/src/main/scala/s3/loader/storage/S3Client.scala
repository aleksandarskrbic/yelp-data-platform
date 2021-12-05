package s3.loader.storage

import zio._
import logstage.LogZIO
import logstage.LogZIO.log
import s3.loader.config.AppConfig
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

final class S3Client(storageConfig: AppConfig.Storage) {
  def get: ZIO[LogZIO, Nothing, AmazonS3] =
    (for {
      _ <- log.info("Creating S3 client")
      client <- ZIO.effect {
                  AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(storageConfig.endpointConfiguration)
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(storageConfig.credentials.toAwsCredentials)
                    .build()
                }
    } yield client).onError(error => log.error(s"Failed to create S3 client $error")).orDie
}

object S3Client {
  lazy val live = (for {
    appConfig    <- ZIO.service[AppConfig]
    storageConfig = appConfig.storage
  } yield new S3Client(storageConfig)).toLayer
}
