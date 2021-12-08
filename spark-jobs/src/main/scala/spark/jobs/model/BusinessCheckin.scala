package spark.jobs.model

import org.apache.spark.sql.Row

final case class BusinessCheckin(
  name: String,
  city: String,
  isOpen: Long,
  review_count: Long,
  stars: Double,
  checkinCount: Long
)

object BusinessCheckin {
  def fromRow(row: Row): BusinessCheckin =
    BusinessCheckin(
      row(0).asInstanceOf[String],
      row(1).asInstanceOf[String],
      row(2).asInstanceOf[Long],
      row(3).asInstanceOf[Long],
      row(4).asInstanceOf[Double],
      row(5).asInstanceOf[String].split(",").length
    )
}
