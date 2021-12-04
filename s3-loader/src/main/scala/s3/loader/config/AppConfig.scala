package s3.loader.config

import zio._
import zio.config.read
import com.typesafe.config.ConfigFactory
import zio.config.typesafe.TypesafeConfigSource
import zio.config.magnolia.DeriveConfigDescriptor
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder

final case class AppConfig(storage: AppConfig.Storage)

object AppConfig {
  private val descriptor = DeriveConfigDescriptor.descriptor[AppConfig]

  final case class Credentials(accessKey: String, secretKey: String) {
    def toAwsCredentials = new BasicAWSCredentials(accessKey, secretKey)
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

  lazy val live = (for {
    rawConfig <- ZIO.effect(ConfigFactory.load().getConfig("s3-loader"))
    configSource <- ZIO.fromEither(TypesafeConfigSource.fromTypesafeConfig(rawConfig))
    config <- ZIO.fromEither(read(descriptor.from(configSource)))
  } yield config).toLayer.orDie
}
