package org.klm.assignment.util

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkJob {

  def sparkConf: SparkConf = {
    new SparkConf()
      .setIfMissing("spark.master", "local[*]")
  }

  lazy val appName: String = this.getClass.getSimpleName.replace("$", "")
  lazy val className: String = getClass.getName.replace("$", "")

  // The implicit SparkSession available for all Spark Jobs
  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .appName(appName)
    .getOrCreate()


  // define logger
  @transient implicit lazy val logger: Logger = org.apache.log4j.LogManager.getLogger(className)

}
