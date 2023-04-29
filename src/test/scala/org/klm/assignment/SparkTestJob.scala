package org.klm.assignment

import org.apache.spark.sql.SparkSession

trait SparkTestJob {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.ui.enabled", false)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

}
