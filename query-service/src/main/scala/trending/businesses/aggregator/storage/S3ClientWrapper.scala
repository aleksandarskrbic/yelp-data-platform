package trending.businesses.aggregator.storage

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import logstage.LogZIO
import logstage.LogZIO.log
import trending.businesses.aggregator.common.AppConfig
import zio._

final class S3ClientWrapper(storageConfig: AppConfig.Storage) {
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
    } yield client).onError(e => log.error(s"Failed to create S3 client $e")).orDie
}

object S3ClientWrapper {
  lazy val live = (for {
    appConfig    <- ZIO.service[AppConfig]
    storageConfig = appConfig.storage
  } yield new S3ClientWrapper(storageConfig)).toLayer
}
