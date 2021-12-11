package spark.jobs.model

import org.apache.spark.sql.Row
import org.joda.time.{LocalDate, Months}

final case class Checkin(businessId: String, checkins: Int)

object Checkin {
  def fromRow(row: Row): Checkin = Checkin(
    row(0).asInstanceOf[String],
    calculateMonths(row(1).asInstanceOf[String])
  )

  private def calculateMonths(field: String) =
    field
      .split(",")
      .map(_.trim.split(" ")(0))
      .collect { case date: String => date }
      .filter(_.nonEmpty)
      .map(LocalDate.parse)
      .count(Months.monthsBetween(_, LocalDate.now()).getMonths < 12)
}
