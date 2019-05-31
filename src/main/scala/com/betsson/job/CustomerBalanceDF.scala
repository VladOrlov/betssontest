package com.betsson.job

import com.betsson.model.CustomerBalance
import com.betsson.util.{JobUtils, SchemaHolder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

object CustomerBalanceDF extends Serializable {

  def process(spark: SparkSession, inputPath: String): Dataset[CustomerBalance] = {
    import spark.implicits._
    implicit val customerBalanceEncoder: Encoder[CustomerBalance] = Encoders.bean(classOf[CustomerBalance])

    val transactionDs = loadTransactions(spark, inputPath)

    val customerBalanceDs = transactionDs.select($"customer_id",
      when($"transaction_type" === "deposit", $"amount")
        .when($"transaction_type" === "win", $"amount")
        .when($"transaction_type" === "bet", negate($"amount"))
        .when($"transaction_type" === "withdraw", negate($"amount"))
        .as("amount"))
      .groupBy("customer_id")
      .sum("amount")
      .select(
        $"customer_id".as("customerId"),
        $"sum(amount)".as("balance"),
        current_timestamp().as("calculationDate"))
      .as[CustomerBalance]

    customerBalanceDs
  }

  private def loadTransactions(spark: SparkSession, inputPath: String) = {
    import spark.implicits._

    JobUtils.getCsvReader(spark, SchemaHolder.getTransactionSchema)
      .csv(s"$inputPath/transactions.csv")
      .select(
        $"transaction_id".as("transaction_id"),
        $"customerid".as("customer_id"),
        $"amount".as("amount"),
        $"transaction_type")
      .where($"amount" > 0)
      .distinct()
  }
}
