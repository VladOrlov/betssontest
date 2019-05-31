package com.betsson.job

import java.time.LocalDateTime

import com.betsson.util.JobParameters
import com.betsson.util.JobUtils.roundAt
import org.apache.spark.RangePartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

import scala.io.Source

object NetProfitByCountry {

  def process(spark: SparkSession, jobParameters: JobParameters): Unit = {
    val broadcastCurrencies = spark.sparkContext.broadcast(getCurrencyRates(jobParameters.baseInputPath))
    val broadcastDateTime = spark.sparkContext.broadcast(LocalDateTime.now())

    val transactionDs = loadTransactions(spark, jobParameters.baseInputPath)
      .mapPartitions(_.map(tr => (tr._2, getAsOperationForBalance(tr, broadcastCurrencies))))
      .reduceByKey((tr1, tr2) => (tr1._1 + tr2._1, tr1._2 + tr2._2))

    val customPartitioner = new RangePartitioner(jobParameters.executionPartitions, transactionDs)

    val pTransactionDs = transactionDs.partitionBy(customPartitioner)
    val customerDs = loadCustomers(spark, jobParameters.baseInputPath).partitionBy(customPartitioner)

    val profitRdd = pTransactionDs.leftOuterJoin(customerDs)
      .mapPartitions(_.map(customerTr => (getCountry(customerTr), getNetProfitAmount(customerTr))))
      .reduceByKey(_ + _)
      .map(profit => s"${profit._1}, ${roundAt(2)(profit._2)}, ${broadcastDateTime.value}")

    val header = spark.sparkContext.parallelize(Array("Country,Net_profit_amount_eur,Calculation_date"))

    header.union(profitRdd)
      .coalesce(jobParameters.writePartitions)
      .saveAsTextFile(jobParameters.outputPath)
  }

  private def getCountry(customerTr: (Int, ((Double, Double), Option[String]))) = {
    customerTr._2._2.getOrElse("Undefined")
  }

  private def getNetProfitAmount(customerTr: (Int, ((Double, Double), Option[String]))): Double = {
    val debitCreditAmount = customerTr._2._1

    val profitAmount = debitCreditAmount._1 - debitCreditAmount._1 * 0.01 - debitCreditAmount._2
    profitAmount
  }

  def getAsOperationForBalance(tr: (String, Int, String, Double), currencies: Broadcast[Map[String, Double]]): (Double, Double) = {
    val amountInEuro: (Double, Double) = currencies.value.get(tr._3)
      .map(rate => if (tr._4 > 0d) (tr._4 / rate, 0d) else (0d, tr._4 * -1 / rate))
      .getOrElse(if (tr._4 > 0d) (tr._4, 0d) else (0d, tr._4 * -1))
    amountInEuro
  }

  private def getCurrencyRates(baseInputPath: String) = {
    val sourceFile = Source.fromFile(s"$baseInputPath/currency.csv")

    val currencies = sourceFile.getLines()
      .filter(_.nonEmpty)
      .drop(1)
      .map(_.split(","))
      .map(ar => (ar.head, ar.last.toDouble))
      .toMap

    sourceFile.close()
    currencies
  }

  private def loadTransactions(spark: SparkSession, inputPath: String) = {
    val rdd = spark.sparkContext.textFile(s"$inputPath/transactions.csv")
    val header = rdd.first()

    val distinctTransactionsRdd =
      rdd.filter(_ != header)
        .filter(isProfitableTransaction)
        .map(parseTransactionLine)
        .distinct()

    distinctTransactionsRdd
  }

  private def parseTransactionLine(row: String) = {
    val fields = row.split(",")
    (fields(0), fields(1).toInt, fields(3), fields(4).toDouble * getTransactionSign(fields(5)))
  }

  private def loadCustomers(spark: SparkSession, inputPath: String) = {
    val rdd = spark.sparkContext.textFile(s"$inputPath/customers.csv")
    val header = rdd.first()

    val customerRdd = rdd.filter(_ != header).map(parseCustomerLine)

    customerRdd
  }

  private def isProfitableTransaction(row: String): Boolean = {
    !row.contains(",0.0,") && (row.contains("bet") || row.contains("win"))
  }

  private def parseCustomerLine(row: String) = {
    val fields = row.split(",")
    (fields(0).toInt, fields(3))
  }

  private def getTransactionSign(transactionType: String): Double = transactionType match {
    case "win" => -1
    case "bet" => 1
    case _ => 0
  }
}