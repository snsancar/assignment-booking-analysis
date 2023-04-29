package org.klm

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.klm.assignment.util.AirportsUtil.Airports

package object assignment {

  val airportsSchema = Encoders.product[Airports].schema

  def setSparkExecutorLogLevel(spark: SparkSession, level: Level): Unit = {
    spark.sparkContext.parallelize(Seq("")).foreachPartition(_ => {
      LogManager.getRootLogger().setLevel(level)
    })
  }
}
