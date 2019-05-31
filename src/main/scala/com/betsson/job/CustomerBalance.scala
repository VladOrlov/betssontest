package com.betsson.job

import java.time.LocalDateTime

import com.betsson.util.JobParameters
import com.betsson.util.JobUtils.roundAt
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession

object CustomerBalance extends Serializable {

  def process(spark: SparkSession, jobParameters: JobParameters): Unit = {
    val transactionsRdd: PairRDDFunctions[String, (Int, Double)] = loadTransactions(spark, jobParameters.baseInputPath)
    val broadcastDateTime = spark.sparkContext.broadcast(LocalDateTime.now())

    val customerTransactionRdd = transactionsRdd.mapValues(t => t._2)
    val customerBalanceRdd = customerTransactionRdd
      .partitionBy(new RangePartitioner(jobParameters.executionPartitions, customerTransactionRdd))
      .reduceByKey(_ + _)
      .map(balance => s"${balance._1}, ${roundAt(2)(balance._2)}, ${broadcastDateTime.value}")

    val header = spark.sparkContext.parallelize(Array("Customerid,Balance,Calculation_date"))

    header.union(customerBalanceRdd)
      .coalesce(jobParameters.writePartitions)
      .saveAsTextFile(jobParameters.outputPath)
  }

  def getTransactionSign(transactionType: String): Double = transactionType match {
    case "deposit" | "win" => 1
    case "bet" | "withdraw" => -1
    case _ => 0
  }

  private def parseLine(row: String): (String, (Int, Double)) = {
    val fields = row.split(",")
    (fields(0), (fields(1).toInt, fields(4).toDouble * getTransactionSign(fields(5))))
  }

  private def loadTransactions(spark: SparkSession, inputPath: String) = {
    val rdd = spark.sparkContext.textFile(s"$inputPath/transactions.csv")
    val header = rdd.first()

    val distinctTransactionsRdd: PairRDDFunctions[String, (Int, Double)] =
      rdd.filter(_ != header)
        .filter(!_.contains(",0.0,"))
        .map(parseLine)
        .distinct()

    distinctTransactionsRdd
  }
}