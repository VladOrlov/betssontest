package com.betsson

import com.betsson.job.{CustomerBalance, NetProfitByCountry}
import com.betsson.util.JobParameters
import org.apache.spark.sql.SparkSession

object JobRunner extends Serializable {

  case class Config(inputPath: String = null,
                    outputPath: String = null,
                    jobType: String = null,
                    executors: Int = 1,
                    writePartitions: Int = 1)

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args).getOrElse(throw new IllegalArgumentException("Arguments provided are incorrect."))

    config.jobType match {
      case "BALANCE" => CustomerBalance.process(buildSparkSession("Customer balance"), getJobConfig(config))
      case "PROFIT" => NetProfitByCountry.process(buildSparkSession("Net profit by country"), getJobConfig(config))
      case _ => throw new UnsupportedOperationException(s"Job not supported! : ${config.jobType}")
  }

    def getJobConfig(config: Config) : JobParameters = {
      JobParameters(
        config.inputPath,
        config.executors*2,
        config.writePartitions,
        config.outputPath
      )
    }
  }

  def buildSparkSession(name: String): SparkSession = {
    SparkSession.builder
      .appName(s"$name processing")
      .getOrCreate()
  }

  private def parseArgs(args: Array[String]): Option[Config] = {
    //    CommandLineParser
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "3.x")

      opt[String]("input") required() action { (x, c) =>
        c.copy(inputPath = x)
      }
      opt[String]("output") required() action { (x, c) =>
        c.copy(outputPath = x)
      }
      opt[String]("job-type") required() action { (x, c) =>
        c.copy(jobType = x)
      }
      opt[String]("executors") required() action { (x, c) =>
        c.copy(executors = x.toInt)
      }
      opt[String]("write-partitions") required() action { (x, c) =>
        c.copy(writePartitions = x.toInt)
      }
    }

    parser.parse(args, Config())
  }
}