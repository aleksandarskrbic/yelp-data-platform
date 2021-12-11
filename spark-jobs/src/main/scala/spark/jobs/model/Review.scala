package spark.jobs.model

import org.apache.spark.sql.Row
import org.joda.time.{LocalDate, Months}

final case class Review(reviewId: String, businessId: String, stars: Double, monthsAgo: Int)

object Review {
  def fromRow(row: Row): Review = Review(
    row(0).asInstanceOf[String],
    row(1).asInstanceOf[String],
    row(2).asInstanceOf[Double],
    calculateMonths(row(3).asInstanceOf[String])
  )

  private def calculateMonths(field: String) = {
    val date = field.split(" ")(0)
    Months.monthsBetween(LocalDate.parse(date), LocalDate.now()).getMonths
  }
}
