package com.betsson.util

import org.apache.spark.sql.types._

object SchemaHolder extends Serializable {

  def getCustomerSchema(): StructType = {
    StructType(Array(
      StructField("customerid", StringType, nullable = false),
      StructField("hashed_name", StringType, nullable = false),
      StructField("registration_date", TimestampType, nullable = false),
      StructField("country_code", StringType, nullable = false)
    ))
  }

  def getTransactionSchema(): StructType = {
    StructType(Array(
      StructField("transaction_id", StringType, nullable = false),
      StructField("customerid", StringType, nullable = false),
      StructField("transaction_date", TimestampType, nullable = false),
      StructField("currency", StringType, nullable = false),
      StructField("amount", DataTypes.createDecimalType(15, 2), nullable = false),
      StructField("transaction_type", StringType, nullable = false)
    ))
  }
}
