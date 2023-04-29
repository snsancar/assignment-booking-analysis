package org.klm.assignment

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.klm.assignment.conf.AppConfig
import org.klm.assignment.util.{AirportsUtil, BookingUtil}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TopDestinationsTest
  extends AnyWordSpec
    with Matchers
    with ArgumentMatchersSugar
    with IdiomaticMockito
    with SparkTestJob {

  "popularDestinations" should {

    "read booking and airport dataset and return insights" in {

      import spark.implicits._

      val result = for {
        conf             <- Try(ConfigFactory.load().resolve())
        appConfig        <- AppConfig.fromConfig(conf)
        booking          <- BookingUtil.readBookingData(appConfig.application.bookingDataPath)
        airports         <- AirportsUtil.readAirportsData(appConfig.application.airportsDataPath)
        nlAirports       <- AirportsUtil.readAirportsDataForNL(airports)
        explodedBooking  <- BookingUtil.explodeTravelRecords(booking)
        nlBookings       <- BookingUtil.filterNLBookings(explodedBooking, nlAirports,
          appConfig.application.fromDate, appConfig.application.toDate)
        enrichedBookings <- BookingUtil.enrichBookings(nlBookings)
        topDestinations  <- BookingUtil.popularDestinations(enrichedBookings, airports)
      } yield topDestinations

      val popularDestinations = result.get
      popularDestinations.printSchema()
      val hungarySpringSeasonCnts = popularDestinations
        .filter(col("destination_country") === "Hungary")
        .select("no_of_passengers", "no_of_adults", "no_of_childs")

      val totalNoOfPassengers = hungarySpringSeasonCnts.select("no_of_passengers").as[Long].collect().head
      totalNoOfPassengers shouldBe 3
      val noOfAdults = hungarySpringSeasonCnts.select("no_of_adults").as[Long].collect().head
      noOfAdults shouldBe 2
      val noOfChilds = hungarySpringSeasonCnts.select("no_of_childs").as[Long].collect().head
      noOfChilds shouldBe 1

    }

    "return Summer as season when departing date is on June, July or August" in {

    }
  }

}
