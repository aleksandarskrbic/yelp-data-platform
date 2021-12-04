package s3.loader

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.UploadPartRequest

import java.io.File

object Main extends App {

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
    .withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration(
        "http://localhost:9000",
        Regions.US_EAST_1.name()
      )
    )
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build()

  import com.amazonaws.services.s3.model.PartETag

  import java.util

  val partETags = new util.ArrayList[PartETag]
  import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest

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
      .withPartSize(partSize)
      .withMD5Digest()
    val uploadResult = s3Client.uploadPart(uploadRequest)
    partETags.add(uploadResult.getPartETag)

    cnt += 1
    filePosition += partSize
  }

  import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest

  val compRequest = new CompleteMultipartUploadRequest(
    bucketName,
    keyName,
    initResponse.getUploadId,
    partETags
  )
  s3Client.completeMultipartUpload(compRequest)
}
