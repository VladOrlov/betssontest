package com.betsson.util

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object JobUtils extends Serializable {

  def getCsvReader(spark: SparkSession, schemaProvider: () => StructType): DataFrameReader = {
    spark.read
      .option("header", "true")
      .option("charset", "UTF8")
      .option("delimiter", ",")
      .schema(schemaProvider())
  }

  def roundAt(p: Int)(n: Double): Double = {
    val s = math pow(10, p); (math round n * s) / s
  }
}