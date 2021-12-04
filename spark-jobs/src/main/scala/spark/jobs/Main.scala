package spark.jobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
