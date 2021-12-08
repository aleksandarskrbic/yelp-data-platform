package s3.loader.service

import zio._
import zio.clock._
import zio.stream._

import java.io.File
import logstage.LogZIO.log
import s3.loader.common.AppConfig
import s3.loader.storage.S3Client
import com.amazonaws.services.s3.model._
import s3.loader.model.{FileSize, FileWrapper, PartDetails, UploadMetadata, UploadPart}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

final class UploadService(s3Client: S3Client, storageConfig: AppConfig.Storage) {
  def upload(fileWrapper: FileWrapper) =
    for {
      _            <- log.info(s"Starting upload for a ${fileWrapper.name}")
      started      <- currentTime(TimeUnit.MILLISECONDS)
      metadata     <- ZIO.succeed(createUploadMetadata(fileWrapper.file))
      initResponse <- s3Client.initMultipartUpload(metadata)
      uploadId      = initResponse.getUploadId
      partDetails  <- ZIO.effect(PartDetails.fromFile(fileWrapper.file))
      parallelism   = getParallelism(fileWrapper.size)
      _            <- log.info(s"Parallelism for ${fileWrapper.name} is $parallelism")
      responses <- ZStream
                     .fromChunk(partDetails.value)
                     .map { case (part, size, offset) =>
                       //add MD5 hash
                       new UploadPartRequest()
                         .withBucketName(metadata.bucket)
                         .withKey(metadata.filename)
                         .withUploadId(uploadId)
                         .withPartNumber(part.value.toInt)
                         .withFileOffset(offset.value)
                         .withFile(fileWrapper.file)
                         .withPartSize(size.value)
                     }
                     .mapMParUnordered(parallelism)(request => s3Client.uploadPart(UploadPart(request)))
                     .runCollect
      completeRequest = createCompleteRequest(metadata, initResponse.getUploadId, responses)
      _              <- s3Client.completeMultipartUpload(completeRequest)
      finished       <- currentTime(TimeUnit.MILLISECONDS)
      total           = (finished - started) / 1000
      _              <- log.info(s"Upload time for ${fileWrapper.name} is ${total}s")
    } yield ()

  private def createUploadMetadata(file: File): UploadMetadata = {
    val filename    = file.getName
    val bucket      = storageConfig.bucket
    val initRequest = new InitiateMultipartUploadRequest(bucket, filename)
    UploadMetadata(file.getName, bucket, initRequest)
  }

  private def createCompleteRequest(
    metadata: UploadMetadata,
    uploadId: String,
    responses: Chunk[UploadPartResult]
  ): CompleteMultipartUploadRequest =
    new CompleteMultipartUploadRequest(
      metadata.bucket,
      metadata.filename,
      uploadId,
      responses.map(_.getPartETag).asJava
    )

  private def getParallelism(fileSize: FileSize): Int =
    fileSize match {
      case FileSize.BigFile    => 64
      case FileSize.MediumFile => 32
      case FileSize.SmallFile  => 16
    }
}

object UploadService {
  lazy val live = (for {
    s3Client     <- ZIO.service[S3Client]
    appConfig    <- ZIO.service[AppConfig]
    storageConfig = appConfig.storage
  } yield new UploadService(s3Client, storageConfig)).toLayer
}
