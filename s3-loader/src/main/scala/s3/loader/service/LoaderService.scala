package s3.loader.service

import zio._
import zio.stream._
import java.io.File
import logstage.LogZIO.log
import s3.loader.common.AppConfig
import s3.loader.model.FileWrapper
import s3.loader.storage.S3Client

final class LoaderService(appConfig: AppConfig, s3Client: S3Client, uploadService: UploadService) {
  def start =
    for {
      _ <- s3Client
             .createBucketIfNotExists(appConfig.storage.bucket)
             .zipPar(s3Client.createBucketIfNotExists(appConfig.storage.processedBucket))
      rootDirectory <- ZIO.effect(createRootDirectory)
      filesToUpload <-
        ZIO
          .effect(Chunk.fromArray(rootDirectory.listFiles()))
          .onError(error => log.error(s"Unable to list files in directory. $error") *> ZIO.succeed(Chunk.empty[File]))
      _ <- ZStream
             .fromChunk(filesToUpload)
             .mapMParUnordered(filesToUpload.size)(file => uploadService.upload(FileWrapper(file)))
             .runDrain
    } yield ()

  private def createRootDirectory: File = new File(appConfig.upload.directory)
}

object LoaderService {
  lazy val live = (for {
    appConfig     <- ZIO.service[AppConfig]
    s3Client      <- ZIO.service[S3Client]
    uploadService <- ZIO.service[UploadService]
  } yield new LoaderService(appConfig, s3Client, uploadService)).toLayer
}
