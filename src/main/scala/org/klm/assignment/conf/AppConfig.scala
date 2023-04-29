package org.klm.assignment.conf

import com.typesafe.config.Config
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource, KebabCase}
import pureconfig.generic.ProductHint
import pureconfig.error.{ ConfigReaderException, ConfigReaderFailures, ThrowableFailure }

import scala.util.Try

case class Application(bookingDataPath: String, airportsDataPath: String,
                       popularDestinationsPath: String, fromDate: String, toDate: String)
case class AppConfig(application: Application)
object AppConfig {

  import pureconfig.generic.auto._

  def fromConfig(config: Config): Try[AppConfig] = {

    // Setup the naming configuration
    implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, KebabCase))

    ConfigSource
      .fromConfig(config)
      .load[AppConfig]
      .left
      .map(err => new ConfigReaderException[AppConfig](err))
      .toTry
  }
}
