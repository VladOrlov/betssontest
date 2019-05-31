package com.betsson.job

import com.betsson.util.JobParameters
import org.apache.spark.sql.SparkSession

object NetProfitByCountryLocalRunner {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()

    val path = System.getProperty("user.dir")
    NetProfitByCountry.process(spark, JobParameters("src/test/resources/", 8, 1, s"$path/processing_result/net_country_profit.csv"))
    spark.close()
  }

  private def getSparkSession() = {
    SparkSession.builder
      .appName("Net country profit processing")
      .master("local[*]")
      .getOrCreate()
  }
}
