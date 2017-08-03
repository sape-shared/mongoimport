package com.test

import com.mongodb.spark.config.WriteConfig
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import com.test

object MongoImportMain {
  def main(arg: Array[String]) {

    val logger = Logger.getLogger(this.getClass())

    if (arg.length < 3) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MongoImportMain <path-to-files> <output-path> <valuation-date>")
      System.exit(1)
    }

    val jobName = "MongoImportMain"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)
    val valuationDate = arg(2)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    val data = sc.textFile(pathToFiles, 4)

    val files = data.flatMap { line =>
      val hsbcTradeId = line.split(",")(0)
      val tradeId = line.split(",")(1)
      DataGenerator.getScalarSchema(hsbcTradeId, tradeId, valuationDate) :::
      DataGenerator.get1DSchema(hsbcTradeId, tradeId, valuationDate) :::
      DataGenerator.get2DSchema(hsbcTradeId, tradeId, valuationDate)
    }

    files.saveAsTextFile(outputPath)
  }

}
