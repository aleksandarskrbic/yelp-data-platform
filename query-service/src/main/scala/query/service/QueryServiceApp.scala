package query.service

import `object`.storage.shared.s3.{S3Client, S3ClientWrapper}
import logstage.LogZIO
import logstage.LogZIO.log
import query.service.common.{AppConfig, Logging}
import zio._
import zio.magic._
import zio.stream._

object QueryServiceApp extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      s3Client  <- ZIO.service[S3Client]
      maybePath <- s3Client.listBucket("yelp-processed", "business_by_city")
      _ <- maybePath match {
             case Some(path) =>
               s3Client.streamFile("yelp-processed", path).foreach(e => ZIO.effect(println(e)))
             case None => ZIO.effect(println("asd"))
           }
      // filename <- s3Client.listBucket("yelp-processed", "businesses_by_city")
      // _        <- log.info(s"$filename")
      // _        <- s3Client.streamFile("yelp-processed/businesses_by_city", filename).foreach(e => ZIO.effect(println(e)))
    } yield ())
      .inject(AppConfig.live, AppConfig.subLayers, Logging.live, S3ClientWrapper.live, S3Client.live, ZEnv.live)
      .exitCode
}
