package s3.loader.common

import zio._
import zio.config.read
import com.typesafe.config.ConfigFactory
import zio.config.typesafe.TypesafeConfigSource
import zio.config.magnolia.DeriveConfigDescriptor
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}

final case class AppConfig(storage: AppConfig.Storage, upload: AppConfig.Upload)

object AppConfig {
  private val descriptor = DeriveConfigDescriptor.descriptor[AppConfig]

  final case class Credentials(accessKey: String, secretKey: String) {
    def toAwsCredentials: AWSStaticCredentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(accessKey, secretKey)
    )
  }

  final case class Upload(directory: String)

  final case class Storage(
    bucket: String,
    processedBucket: String,
    region: String,
    serviceEndpoint: String,
    credentials: Credentials
  ) {
    def endpointConfiguration: AwsClientBuilder.EndpointConfiguration =
      new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region)
  }

  lazy val live = (for {
    rawConfig    <- ZIO.effect(ConfigFactory.load().getConfig("s3-loader"))
    configSource <- ZIO.fromEither(TypesafeConfigSource.fromTypesafeConfig(rawConfig))
    config       <- ZIO.fromEither(read(descriptor.from(configSource)))
  } yield config).toLayer.orDie

  lazy val subLayers =
    ZLayer.service[AppConfig].map { hasConfig =>
      val config = hasConfig.get[AppConfig]
      Has(config.storage.credentials.toAwsCredentials) ++ Has(config.storage.endpointConfiguration)
    }
}
