package org.klm.assignment.util

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.klm.assignment.airportsSchema

import scala.util.Try

object AirportsUtil {

  case class Airports(airportId: Int, name: String, city: String, country: String, IATA: String, ICAO: String,
                      latitude: BigDecimal, longitude: BigDecimal, altitude: Int, timeZone: Int, dst: String,
                      tzDbTimeZone: String, airportType: String, source: String)


  def readAirportsData(path: String)(implicit spark: SparkSession): Try[DataFrame] =
    Try {
      spark.read
        .option("header", false)
        .schema(airportsSchema)
        .csv(path)
        .filter(col("IATA") =!= "\\N")//Filter records which has Not known Airport codes
        .cache()//Cache it to re-use it on different transformations
    }

  def readAirportsDataForNL(airports: DataFrame): Try[DataFrame] =
    Try {
      airports
        .filter(col("country") === "Netherlands")//only NL Airports
    }

}
