// src/main/scala/flightpipeline/util/ParsingUtils.scala
package flightpipeline.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column}
import org.apache.spark.sql.types._

object ParsingUtils {

  def nullIfMissing(c: Column): Column =
    when(trim(c) === lit("") || trim(c) === lit("M"), lit(null).cast(StringType)).otherwise(c)

  def toTimestampFromDateTime(dateCol: Column, timeCol: Column): Column =
    to_timestamp(concat(date_format(to_date(dateCol, "yyyyMMdd"), "yyyy-MM-dd"), lpad(timeCol, 4, "0")), "yyyy-MM-ddHHmm")

  val extractAltitudes = udf { (sc: String) =>
    if (sc == null) Seq.empty[Int]
    else {
      val re = "(?<!/)(\\d{3})(?!/)".r
      re.findAllIn(sc).toSeq.map(_.toInt * 100) // centaines de pieds → pieds
    }
  }

  val countSeq = udf { (sc: String) =>
    if (sc == null || sc.trim.isEmpty || sc.trim == "M") 0
    else sc.trim.split("\\s+").length
  }

  val hasCB  = udf((sc: String) => Option(sc).exists(_.contains("CB")))
  val hasTCU = udf((sc: String) => Option(sc).exists(_.contains("TCU")))

  val extractWeatherScores = udf { (wt: String) =>
    if (wt == null || wt.trim.isEmpty) Map.empty[String, Int]
    else {
      "([+-]?)([A-Z]{2})".r.findAllMatchIn(wt).map { m =>
        val w = m.group(1) match { case "-" => 1; case "" => 2; case "+" => 4 }
        m.group(2) -> w
      }.toMap
    }
  }

  val weatherPairs: Seq[String] =
    Seq("BR","DU","DZ","FG","FU","GR","GS","HZ","IC","PE","PL","PO","RA","SA","SG","SH","SN","SQ","SS","TS","UP","VA","VC","FZ","BL","DS","DR","FC")
      .distinct
}
