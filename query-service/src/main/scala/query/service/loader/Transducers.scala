package query.service.loader

import zio._
import zio.stream._
import scala.util.Try
import query.service.model._

object Transducers {
  val businessByCityCount: ZTransducer[Any, Throwable, String, BusinessByCityCount] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map { case city :: count :: _ =>
        Try(BusinessByCityCount(city, count.toLong)).toOption
      }
    }

  val businessByIsOpen: ZTransducer[Any, Throwable, String, OpenedBusinessStats] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map { case isOpen :: count :: _ =>
        Try {
          isOpen match {
            case "1" => OpenedBusinessStats(isOpen = true, count.toLong)
            case "0" => OpenedBusinessStats(isOpen = false, count.toLong)
          }
        }.toOption
      }
    }

  val trendingBusinesses: ZTransducer[Any, Throwable, String, Business] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map {
        case businessId :: name :: city :: positiveReviewCount :: checkinCount :: _ =>
          Try(Business(businessId, name, city, positiveReviewCount.toLong, checkinCount.toLong)).toOption
      }
    }

  private def csvTransducer[O](fn: String => Task[Option[O]]): ZTransducer[Any, Throwable, String, O] =
    ZTransducer.fromFunctionM[Any, Throwable, String, Option[O]](fn).filter(_.isDefined).map(_.get)
}
