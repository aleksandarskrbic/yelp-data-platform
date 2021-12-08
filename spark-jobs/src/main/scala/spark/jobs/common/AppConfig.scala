package spark.jobs.common

import zio._
import zio.config.read
import com.typesafe.config.ConfigFactory
import zio.config.typesafe.TypesafeConfigSource
import zio.config.magnolia.DeriveConfigDescriptor
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}

final case class AppConfig(storage: AppConfig.Storage, source: AppConfig.Sources)

object AppConfig {
  private val descriptor = DeriveConfigDescriptor.descriptor[AppConfig]

  final case class Credentials(accessKey: String, secretKey: String) {
    def toAwsCredentials = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(accessKey, secretKey)
    )
  }

  final case class Storage(
    bucket: String,
    region: String,
    serviceEndpoint: String,
    credentials: Credentials
  ) {
    def endpointConfiguration =
      new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region)
  }

  final case class S3Path(value: String) extends AnyVal
  final case class Sources(users: S3Path, reviews: S3Path, checkins: S3Path, businesses: S3Path)

  lazy val live = (for {
    rawConfig    <- ZIO.effect(ConfigFactory.load().getConfig("spark-jobs"))
    configSource <- ZIO.fromEither(TypesafeConfigSource.fromTypesafeConfig(rawConfig))
    config       <- ZIO.fromEither(read(descriptor.from(configSource)))
  } yield config).toLayer.orDie
}
