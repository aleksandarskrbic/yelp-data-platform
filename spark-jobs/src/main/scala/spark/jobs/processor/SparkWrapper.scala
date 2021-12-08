package spark.jobs.processor

import zio._
import spark.jobs.common.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final class SparkWrapper(sparkSession: SparkSession) {}

object SparkWrapper {
  lazy val live = (for {
    appConfig <- ZIO.service[AppConfig]
    storage    = appConfig.storage
    sparkConf = new SparkConf()
                  .setAppName("MinIO example")
                  .setMaster("local")
                  .set("spark.hadoop.fs.s3a.endpoint", storage.serviceEndpoint)
                  .set("spark.hadoop.fs.s3a.access.key", storage.credentials.accessKey)
                  .set("spark.hadoop.fs.s3a.secret.key", storage.credentials.accessKey)
                  .set("spark.hadoop.fs.s3a.fast.upload", "true")
                  .set("spark.hadoop.fs.s3a.path.style.access", "true")
                  .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkSession <- ZIO.effect(SparkSession.builder.config(sparkConf).getOrCreate())
  } yield new SparkWrapper(sparkSession))
}
