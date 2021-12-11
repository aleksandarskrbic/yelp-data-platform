package spark.jobs.model

import java.time.Year
import org.apache.spark.sql.Row

final case class UserDetails(userId: String, reviewCount: Long, averageStars: Double, yelpingFor: Int, friends: Int)

object UserDetails {
  def fromRow(row: Row): UserDetails =
    UserDetails(
      row(0).asInstanceOf[String],
      row(1).asInstanceOf[Long],
      row(2).asInstanceOf[Double],
      Year.now.getValue - row(3).asInstanceOf[String].split(" ")(0).split("-")(0).toInt,
      row(4).asInstanceOf[String].split(",").length
    )
}
