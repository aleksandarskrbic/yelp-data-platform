package spark.jobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.jobs.adapter.SparkWrapper.createSparkConf

object Test extends App {
  val sparkConf = new SparkConf()
    .setAppName("MinIO example")
    .setMaster("local[*]")
    .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.fast.upload", "true")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  val sparkSession = SparkSession.builder
    .config(sparkConf)
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val reviewDF = sparkSession.read.format("json").load("s3a://yelp-raw/yelp_academic_dataset_review.json")
  val f = reviewDF
    .select("text", "stars")
    .filter("stars > 3")
    .map(row => row(0).asInstanceOf[String].replaceAll("\\W+", " ").toLowerCase)
    .flatMap(a => a.split(" "))

  f.show(10)
}
