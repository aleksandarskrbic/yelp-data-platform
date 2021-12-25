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
      ZIO.effect(line.split(",").toList).map { case _ :: name :: city :: positiveReviewCount :: checkinCount :: _ =>
        Try(Business(name, city, positiveReviewCount.toLong, checkinCount.toLong)).toOption
      }
    }

  val checkinStats: ZTransducer[Any, Throwable, String, CheckinStats] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map { case _ :: _ :: year :: month :: day :: hour :: _ =>
        Try(CheckinStats(year.toInt, month.toInt, day.toInt, hour.toInt)).toOption
      }
    }

  val userDetails: ZTransducer[Any, Throwable, String, UserDetails] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map {
        case _ :: reviewCount :: averageStars :: yelpingFor :: friendsCount :: _ =>
          Try(UserDetails(reviewCount.toLong, averageStars.toInt, yelpingFor.toInt, friendsCount.toInt)).toOption
      }
    }

  val businessCheckinDetails: ZTransducer[Any, Throwable, String, BusinessCheckinDetails] =
    csvTransducer { line =>
      ZIO.effect(line.split(",").toList).map {
        case name :: city :: isOpen :: reviewCount :: stars :: checkinCount :: _ =>
          Try {
            val businessCheckinDetails = BusinessCheckinDetails(
              name,
              city,
              isOpen = true,
              reviewCount.toLong,
              stars.toDouble,
              checkinCount.toLong
            )
            isOpen match {
              case "1" => businessCheckinDetails
              case "0" => businessCheckinDetails.copy(isOpen = false)
            }
          }.toOption
      }
    }

  private def csvTransducer[O](fn: String => Task[Option[O]]): ZTransducer[Any, Throwable, String, O] =
    ZTransducer.fromFunctionM[Any, Throwable, String, Option[O]](fn).filter(_.isDefined).map(_.get)
}
