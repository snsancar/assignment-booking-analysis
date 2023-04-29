package org.klm.assignment

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Level
import org.klm.assignment.conf.AppConfig
import org.klm.assignment.util.{AirportsUtil, BookingUtil, SparkJob}

import scala.util.{Failure, Success, Try}

object TopDestinations extends App with SparkJob {

  setSparkExecutorLogLevel(spark, Level.WARN)

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

  result match {
    case Success(popularDestinations) =>
      popularDestinations.show(100, false)
//      popularDestinations.write.option("header", true).csv(appConfig.application.popularDestinationsPath)
    case Failure(exception) => throw exception
  }
}
