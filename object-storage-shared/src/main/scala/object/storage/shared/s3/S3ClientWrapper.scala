package `object`.storage.shared.s3

import zio._
import logstage.LogZIO
import logstage.LogZIO.log
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

final class S3ClientWrapper(
  awsCredentials: AWSStaticCredentialsProvider,
  endpointConfiguration: AwsClientBuilder.EndpointConfiguration
) {
  def get: ZIO[LogZIO, Nothing, AmazonS3] =
    (for {
      _ <- log.info("Creating S3 client")
      client <- ZIO.effect {
                  AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(endpointConfiguration)
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(awsCredentials)
                    .build()
                }
    } yield client).onError(error => log.error(s"Failed to create S3 client $error")).orDie
}

object S3ClientWrapper {
  lazy val live = (for {
    awsCredentials        <- ZIO.service[AWSStaticCredentialsProvider]
    endpointConfiguration <- ZIO.service[AwsClientBuilder.EndpointConfiguration]
  } yield new S3ClientWrapper(awsCredentials, endpointConfiguration)).toLayer
}
