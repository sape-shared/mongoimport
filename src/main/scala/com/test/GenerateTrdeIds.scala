package com.test

import java.io.{BufferedWriter, File, FileWriter}

import scala.annotation.tailrec

/**
  * Created by bgup12 on 8/2/2017.
  */
object GenerateTrdeIds extends App {

  val startTradeId = args(0).toInt
  val numberOfTrades = args(1).toInt
  val numberOfFiles = args(2).toInt
  val path = args(3)

  def generateTradeId(startFromTradeId: Int, numOfTradeId: Int, numOfFiles: Int, path: String, fileNamePrefix: String) = {
    @tailrec
    def generate(startFromTradeId: Int, endWithTradeId: Int, batchSize: Int, reducedBatchSize: Int, fileNum: Int, bufferWriter: BufferedWriter): String = {
      if (startFromTradeId == endWithTradeId) {
        tradeId(startFromTradeId, bufferWriter)
        bufferWriter.close()
        "done"
      }
      else {
        val newBufferWriter = {
          if (reducedBatchSize == 0) {
            bufferWriter.close()
            new BufferedWriter(new FileWriter(new File(path + "/" + fileNamePrefix + "_" + (fileNum + 1))))
          }
          else
            bufferWriter
        }
        val newFileNum = if (reducedBatchSize == 0) fileNum + 1 else fileNum
        val newReducedBatchSize = if (reducedBatchSize == 0) batchSize-1 else reducedBatchSize - 1
        tradeId(startFromTradeId, newBufferWriter)
        generate(startFromTradeId + 1, endWithTradeId, batchSize, newReducedBatchSize, newFileNum, newBufferWriter)
      }
    }

    def tradeId(tradeIdNum: Int, bufferWriter: BufferedWriter) = {
      val tradeId = {
        if (tradeIdNum % 5 == 0) "000000237974" + "X" + tradeIdNum + "," + tradeIdNum
        else if (tradeIdNum % 1 == 0) "000000237974" + "B" + tradeIdNum + "," + tradeIdNum
        else if (tradeIdNum % 2 == 0) "000000237974" + "F" + tradeIdNum + "," + tradeIdNum
        else if (tradeIdNum % 3 == 0) "000000237974" + "L" + tradeIdNum + "," + tradeIdNum
        else "000000237974" + tradeIdNum + "," + tradeIdNum
      }
      bufferWriter.write(tradeId)
      bufferWriter.write("\n")
    }

    val initBufferWriter = new BufferedWriter(new FileWriter(new File(path + "/" + fileNamePrefix + "_0")))
    val batchSize: Int = {
      if (numOfTradeId % numOfFiles == 0) numOfTradeId / numOfFiles else numOfTradeId / numOfFiles + 1
    }
    val reducedBatchSize = batchSize
    generate(startFromTradeId, startFromTradeId + numOfTradeId - 1, batchSize, reducedBatchSize, 0, initBufferWriter)
  }

  generateTradeId(startTradeId, numberOfTrades, numberOfFiles, path, "trade_id")
}




