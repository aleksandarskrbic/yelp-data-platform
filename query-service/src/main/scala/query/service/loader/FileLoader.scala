package query.service.loader

import zio._
import zio.stream._
import `object`.storage.shared.s3.S3Client

final class FileLoader(bucket: String, s3Client: S3Client) {

  def businessByCityStream() =
    ZStream
      .fromEffect(s3Client.listBucket(bucket, "business_by_city"))
      .flatMap {
        case Some(path) => s3Client.streamFile("yelp-processed", path).mapError(e => "")
        case None       => ZStream.fail("asd")
      }
      .mapError(e => "")

  /*    (for {
      maybePath <- s3Client.listBucket(bucket, "business_by_city")
      a <- maybePath match {
             case Some(path) =>
               s3Client.streamFile("yelp-processed", path)
             case None => ZIO.fail("asd")
           }
    } yield ())*/

}
