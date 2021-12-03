import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.UploadPartRequest
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File

object Job extends App {

  val sparkConf = new SparkConf()
    .setAppName("MinIO example")
    .setMaster("local")
    .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.fast.upload", "true")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  val spark = SparkSession
    .builder
    .config(sparkConf)
    .getOrCreate()

  val df = spark.read.format("json").load("s3a://asd/yelp_academic_dataset_review.json")
  df.show(10)
}

object Main extends App {

  import com.amazonaws.auth.AWSCredentials
  import com.amazonaws.auth.BasicAWSCredentials

  val bucketName = "asd"
  val keyName = "yelp_academic_dataset_review.json"
  val filePath = "data/yelp_dataset/yelp_academic_dataset_review.json"

  val file = new File(filePath)
  val contentLength = file.length
  var partSize: Long = 5 * 1024 * 1024
  println(contentLength)

  val credentials = new BasicAWSCredentials("minioadmin", "minioadmin")
  val clientConfiguration = new ClientConfiguration();

  val s3Client = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build()

  import com.amazonaws.services.s3.model.PartETag
  import java.util

  val partETags = new util.ArrayList[PartETag]
  import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
  import com.amazonaws.services.s3.model.InitiateMultipartUploadResult

  val initRequest = new InitiateMultipartUploadRequest(bucketName, keyName)
  val initResponse = s3Client.initiateMultipartUpload(initRequest)
  var filePosition = 0L
  var cnt = 1
  while (filePosition < contentLength) {
    partSize = Math.min(partSize, (contentLength - filePosition))

    val uploadRequest = new UploadPartRequest()
      .withBucketName(bucketName)
      .withKey(keyName)
      .withUploadId(initResponse.getUploadId)
      .withPartNumber(cnt)
      .withFileOffset(filePosition)
      .withFile(file)
      .withPartSize(partSize);

    import com.amazonaws.services.s3.model.UploadPartResult
    val uploadResult = s3Client.uploadPart(uploadRequest)
    partETags.add(uploadResult.getPartETag)

    cnt += 1
    filePosition += partSize
  }

  import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest

  val compRequest = new CompleteMultipartUploadRequest(bucketName, keyName, initResponse.getUploadId, partETags)
  s3Client.completeMultipartUpload(compRequest)
}
