package org.klm.assignment.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object BookingUtil {

  /**
   * Method used to read the bookings data based on path ( could be local or HDFS path )
   * @param path String
   * @param spark SparkSession
   * @return DataFrame
   */
  def readBookingData(path: String)(implicit spark: SparkSession): Try[DataFrame] = {
    Try {
      spark.read.json(path)
    }
  }

  def explodeTravelRecords(booking: DataFrame): Try[DataFrame] =
    Try{
      booking
        .select(col("*"), explode(col("event.DataElement.travelrecord.passengersList")).alias("passengers"))
        .select(col("*"), explode(col("event.DataElement.travelrecord.productsList")).alias("products"))
        .filter(col("products.bookingStatus") === "CONFIRMED")//to get only confirmed bookings
        .filter(col("products.flight.operatingAirline") === "KL")// to get only KL Flights
//        .withColumn("nbPassengers", col("event.DataElement.travelrecord.nbPassengers"))
        .withColumn("passengerType", col("passengers.passengerType"))
        .withColumn("destination_airport", col("products.flight.destinationAirport"))
        .withColumn("origin_airport", col("products.flight.originAirport"))
        .withColumn("departureDateNLTz", to_date(from_utc_timestamp(col("products.flight.departureDate"), "Europe/Amsterdam")))
        .select("passengers", "products", "passengerType", "destination_airport", "origin_airport", "departureDateNLTz")
    }

  /**
   * Function used to filter bookings only from Netherlands and also filter out based on Start and End Date.
   * @param explodedBookings DataFrame
   * @param nlAirports DataFrame
   * @param fromDate String
   * @param endDate String
   * @return
   */
  def filterNLBookings(explodedBookings: DataFrame, nlAirports: DataFrame, fromDate: String,
                       endDate: String): Try[DataFrame] =
    Try {
      explodedBookings
        .join(broadcast(nlAirports), explodedBookings("origin_airport") === nlAirports("IATA")) // to get only Netherlands Airports
        .drop("IATA")
        .filter(col("departureDateNLTz") >= lit(fromDate)
          && col("departureDateNLTz") <= lit(endDate))
        .select("passengers", "products", "passengerType", "destination_airport", "departureDateNLTz")
    }

  /**
   * Function used to enrich bookings dataframe by adding columns like (day_of_the_week, season )
   * @param nlBookings
   * @return
   */
  def enrichBookings(nlBookings: DataFrame): Try[DataFrame] =
    Try {
      nlBookings
        .withColumn("day_of_the_week", date_format(col("departureDateNLTz"), "EEEE"))
        .withColumn("month_of_year", month(col("departureDateNLTz")))
        .withColumn("season", when(col("month_of_year").isin(3,4,5), lit("Spring"))
          .when(col("month_of_year").isin(6,7,8), lit("Summer"))
          .when(col("month_of_year").isin(9,10,11), lit("Autumn"))
          .otherwise(lit("Winter")))
        .select("passengerType","destination_airport", "day_of_the_week", "season")
    }

  def popularDestinations(enrichedBookings: DataFrame, airports: DataFrame): Try[DataFrame] =
    Try {
      val bookingWithDestCountry = enrichedBookings
        .join(airports, enrichedBookings("destination_airport") === airports("IATA"))
        .select(
          col("country").as("destination_country"),
          col("passengerType"),
          col("destination_airport"),
          col("day_of_the_week"),
          col("season")
        )

      bookingWithDestCountry
        .groupBy("destination_country", "day_of_the_week", "season")
        .agg(
          count("passengerType").alias("no_of_passengers"),
          sum(when(col("passengerType")===lit("ADT"), lit(1)).otherwise(lit(0))).as("no_of_adults"),
          sum(when(col("passengerType")===lit("CHD"), lit(1)).otherwise(lit(0))).as("no_of_childs"),
        )
        .orderBy(col("no_of_passengers").desc)
    }
}
