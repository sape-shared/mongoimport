package com.test

import java.text.SimpleDateFormat
import java.util.Calendar


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger


object MongoImportMain {
  def main(arg: Array[String]) {

    val logger = Logger.getLogger(this.getClass())

    if (arg.length < 4) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MongoImportMain <path-to-files> <output-path> <valuation-date> <no-of-days>")
      System.exit(1)
    }

    val jobName = "MongoImportMain"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val pathToFiles = arg(0)
    val outputPath = arg(1)
    val valuationDate = arg(2)

    val noOfDays = arg(3).toInt
    val data = sc.textFile(pathToFiles).cache()

    for (i <- 0 to noOfDays-1) {
      val outputFolder = getIncrementedDate(valuationDate, i)

      logger.info("=> jobName \"" + jobName + "\"")
      logger.info("=> pathToFiles \"" + pathToFiles + "\"")

      val files = data.flatMap { line =>
        val hsbcTradeId = line.split(",")(0)
        val tradeId = line.split(",")(1)
        val bookId = hsbcTradeId.substring(10,14)
        DataGenerator.getScalarSchema(hsbcTradeId, tradeId,bookId, valuationDate) :::
          DataGenerator.get1DSchema(hsbcTradeId, tradeId,bookId, valuationDate) :::
          DataGenerator.get2DSchema(hsbcTradeId, tradeId,bookId, valuationDate)
      }

      files.saveAsTextFile(outputPath+"/"+outputFolder)
    }

  }

  import java.util.Date

  def getIncrementedDate(valuationDate: String, noOfDays: Int): String = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: Date = formatter.parse(valuationDate)
    val c: Calendar = Calendar.getInstance
    c.setTime(date) // Now use today date.
    c.add(Calendar.DATE, noOfDays)
    formatter.format(c.getTime)
  }
}
