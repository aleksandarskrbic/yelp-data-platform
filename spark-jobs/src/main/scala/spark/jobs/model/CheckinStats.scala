package spark.jobs.model

import org.apache.spark.sql.Row

final case class CheckinStats(businessId: String, year: Int, month: Int, day: Int, hour: Int)

object CheckinStats {
  def fromRow(row: Row): CheckinStats = {
    val field = row(1).asInstanceOf[String].split(",")
    CheckinStats(
      row(0).asInstanceOf[String],
      field
        .map(_.trim.split(" ")(0).split("-")(0).toInt)
        .groupBy(x => x)
        .maxBy(_._2.length)
        ._1,
      field
        .map(_.trim.split(" ")(0).split("-")(1).toInt)
        .groupBy(x => x)
        .maxBy(_._2.length)
        ._1,
      field
        .map(_.trim.split(" ")(0).split("-")(2).toInt)
        .groupBy(x => x)
        .maxBy(_._2.length)
        ._1,
      field
        .map(_.trim.split(" ")(1).split(":")(0).toInt)
        .groupBy(x => x)
        .maxBy(_._2.length)
        ._1
    )
  }
}
