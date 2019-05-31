package com.betsson.job

import com.betsson.util.JobParameters
import org.apache.spark.sql.SparkSession

object CustomerBalanceLocalRunner {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()

    val path = System.getProperty("user.dir")
    CustomerBalance.process(spark, JobParameters("src/test/resources/", 8, 1, s"$path/processing_result/customers_balance.csv"))
    spark.close()
  }

  private def getSparkSession() = {
    SparkSession.builder
      .appName("processing")
      .master("local[*]")
      .getOrCreate()
  }
}