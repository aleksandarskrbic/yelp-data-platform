package spark.jobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.storage.{FileRepository, S3ClientWrapper}
import zio.{ExitCode, URIO, ZIO}
import zio.magic._

object Main {
  val sparkConf = new SparkConf()
    .setAppName("MinIO example")
    .setMaster("local")
    .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.fast.upload", "true")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  val spark = SparkSession.builder
    .config(sparkConf)
    .getOrCreate()

  val df = spark.read
    .format("json")
    .load("s3a://asd/yelp_academic_dataset_review.json")
  df.show(10)
}

object Test extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      config    <- ZIO.service[AppConfig]
      dataframes = config.source
      _         <- ZIO.effect(println(dataframes))
    } yield ()).inject(AppConfig.live, Logging.live, S3ClientWrapper.live, FileRepository.live).exitCode
}
