package com.test
/**
  * Created by rmish3 on 8/3/2017.
  */
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex


object DataGenerator {
  val reg = """\<([^>]+)\>""".r

   val schema2D  =
   """{  "measure":{   "name":"<RISK_TYPE>",   "class":"SENSITIVE",   "snapshotcode": "BOD",   "stateStatus": "OK",   "unitName" : "INR",   "unitAssetType":"CURRENCY"  },  "riskFactor1" : {   "riskFactor1Type" :"ICURVE",   "curveCurrency":"INR",   "curveIndex": "RAD04"},  "riskFactor2" : {  },  "axisDetails": {   "xAxis" : {      "axisType" : "TENOR",      "axisUnit" : "YEAR"   },   "yAxis" : {      "axisType" : "TENOR",      "axisUnit" : "YEAR"   },   "zAxis" : {   },   "axisPoints" : [    {       "xAxisPoints" : {        "label" : "1W",        "value" : "1"       },       "yAxisPoints" : {        "label" : "1W",        "value" : "1"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_1>    },    {       "xAxisPoints" : {        "label" : "2W",        "value" : "2"       },       "yAxisPoints" : {        "label" : "2W",        "value" : "2"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_2>    },    {       "xAxisPoints" : {        "label" : "3W",        "value" : "3"       },       "yAxisPoints" : {        "label" : "3W",        "value" : "3"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_3>    },    {       "xAxisPoints" : {        "label" : "4W",        "value" : "4"       },       "yAxisPoints" : {        "label" : "4W",        "value" : "4"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_4>    },    {       "xAxisPoints" : {        "label" : "5W",        "value" : "5"       },       "yAxisPoints" : {        "label" : "5W",        "value" : "5"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_5>    },    {       "xAxisPoints" : {        "label" : "6M",        "value" : "6"       },       "yAxisPoints" : {        "label" : "6M",        "value" : "6"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_6>    },    {       "xAxisPoints" : {        "label" : "7M",        "value" : "7"       },       "yAxisPoints" : {        "label" : "7M",        "value" : "7"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_7>    },    {       "xAxisPoints" : {        "label" : "8M",        "value" : "8"       },       "yAxisPoints" : {        "label" : "8M",        "value" : "8"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_8>    },    {       "xAxisPoints" : {        "label" : "9M",        "value" : "9"       },       "yAxisPoints" : {        "label" : "9M",        "value" : "9"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_9>    },    {       "xAxisPoints" : {        "label" : "10M",        "value" : "10"       },       "yAxisPoints" : {        "label" : "10M",        "value" : "10"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_10>    },    {       "xAxisPoints" : {        "label" : "11M",        "value" : "11"       },       "yAxisPoints" : {        "label" : "11M",        "value" : "11"       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_11>    },    {       "xAxisPoints" : {        "label" : "12M",        "value" : "12"       },       "yAxisPoints" : {        "label" : "12M",        "value" : "12"       },       "zAxisPoints" : {       },       "measureValue" :<MEASURE_VALUE_12>    },    {       "xAxisPoints" : {        "label" : "1Y",        "value" : "1"       },       "yAxisPoints" : {        "label" : "1Y",        "value" : "1"       },       "zAxisPoints" : {       },       "measureValue" :<MEASURE_VALUE_13>    }   ]  },  "scenario" : {   "shift" : "0.0005"  },  "instructionSet" : {  },  "valuationContext" : {   "valuationContextId" : "12345678901111111",   "description" : "XXXXXXXXXXXXXX",   "batchName" : "XXXXXXXXXXXXXX",   "batchTimeStamp" : 20170526220200,   "transformTimeStamp" : 20170526170500,   "sourceRiskEngineName" : "SUMMIT"  },  "marketContext" : {   "marketContextName" : "20170526",   "marketContextSetId" : "AAAI0",   "marketContextTimeStamp" : "20170526000000"  },  "riskSource" : {   "hsbctradeId" : "<HSBC_TRADE_ID>",   "tradeId" : "<TRADE_ID>",   "sourceSystemClass" : "FI",   "sourceSystemName" : "AAAI0",   "sourceIdentifier" : "launch",   "sourceFormatType" : "FILE",   "riskSourceType" : "TRADE"  },  "valuationDate" : <VALUATION_DATE>,  "validTo" : 99991231235959,  "validFrom" : 2017080212000} """.stripMargin.replaceAll("\n", " ")

  val schema1D =
    """{  "measure":{   "name":"<RISK_TYPE>",   "class":"SENSITIVE",   "snapshotcode": "BOD",   "stateStatus": "OK",   "unitName" : "INR",   "unitAssetType":"CURRENCY"  },  "riskFactor1" : {   "riskFactor1Type" :"ICURVE",   "curveCurrency":"INR",   "curveIndex": "DAT07"  },  "riskFactor2" : {  },  "axisDetails": {   "xAxis" : {      "axisType" : "TENOR",      "axisUnit" : "YEAR"   },   "yAxis" : {   },   "zAxis" : {   },   "axisPoints" : [    {       "xAxisPoints" : {       "label" : "1M",        "value" : "1"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_1>    },    {       "xAxisPoints" : {        "label" : "2M",        "value" : "2"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_2>    },    {       "xAxisPoints" : {        "label" : "3M",        "value" : "3"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_3>    },    {       "xAxisPoints" : {        "label" : "4M",        "value" : "4"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_4>    },    {       "xAxisPoints" : {        "label" : "5M",        "value" : "5"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_5>    },    {       "xAxisPoints" : {        "label" : "6M",        "value" : "6"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_6>    },    {       "xAxisPoints" : {        "label" : "7M",        "value" : "7"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_7>    },    {       "xAxisPoints" : {        "label" : "8M",        "value" : "8"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_8>    },    {       "xAxisPoints" : {        "label" : "9M",        "value" : "9"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_9>    },    {       "xAxisPoints" : {        "label" : "10M",        "value" : "10"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_10>    },    {       "xAxisPoints" : {        "label" : "11M",        "value" : "11"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_11>    },    {       "xAxisPoints" : {        "label" : "1Y",        "value" : "1"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_12>    },    {       "xAxisPoints" : {        "label" : "2Y",        "value" : "2"       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_13>    }   ]  },  "scenario" : {   "shift" : "0.00006"  },  "instructionSet" : {  },  "valuationContext" : {   "valuationContextId" : "789455501112221",   "description" : "YYYYYYYYYYYYY",   "batchName" : "YYYYYYYYYYYYY",   "batchTimeStamp" : 20170526220200,   "transformTimeStamp" : 20170526170500,   "sourceRiskEngineName" : "SUMMIT"  },  "marketContext" : {   "marketContextName" : "20150526",   "marketContextSetId" : "BBBQ0",   "marketContextTimeStamp" : "20150526000000"  },  "riskSource" : {   "hsbctradeId" : "<HSBC_TRADE_ID>",   "tradeId" : "<TRADE_ID>",   "sourceSystemClass" : "FI",   "sourceSystemName" : "BBBQ0",   "sourceIdentifier" : "launch",   "sourceFormatType" : "FILE",   "riskSourceType" : "TRADE"  },  "valuationDate" : <VALUATION_DATE>,  "validTo" : 99991231235959,  "validFrom" : 2017080212000} """.stripMargin.replaceAll("\n", " ")

  val schemaScalar =
    """{  "measure":{   "name":"<RISK_TYPE>",   "class":"SENSITIVE",   "snapshotcode": "BOD",   "stateStatus": "OK",   "unitName" : "INR",   "unitAssetType":"CURRENCY"  },  "riskFactor1" : {   "riskFactor1Type" :"ICURVE",   "curveCurrency":"INR",   "curveIndex": "YOT02"  },  "riskFactor2" : {  },  "axisDetails": {   "xAxis" : {   },   "yAxis" : {   },   "zAxis" : {   },   "axisPoints" : [    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_1>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_2>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_3>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_4>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_5>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_6>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_7>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_8>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_9>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_10>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_11>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_12>    },    {       "xAxisPoints" : {       },       "yAxisPoints" : {       },       "zAxisPoints" : {       },       "measureValue" : <MEASURE_VALUE_13>    }   ]  },  "scenario" : {   "shift" : "0.00004"  },  "instructionSet" : {  },  "valuationContext" : {   "valuationContextId" : "555748901112221",   "description" : "YYYYYYYYYYYYY",   "batchName" : "YYYYYYYYYYYYY",   "batchTimeStamp" : 20170526220200,   "transformTimeStamp" : 20170526170500,   "sourceRiskEngineName" : "SUMMIT"  },  "marketContext" : {   "marketContextName" : "20150526",   "marketContextSetId" : "CCCV0",   "marketContextTimeStamp" : "20150526000000"  },  "riskSource" : {   "hsbctradeId" : "<HSBC_TRADE_ID>",   "tradeId" : "<TRADE_ID>",   "sourceSystemClass" : "FI",   "sourceSystemName" : "CCCV0",   "sourceIdentifier" : "launch",   "sourceFormatType" : "FILE",   "riskSourceType" : "TRADE"  },  "valuationDate" : <VALUATION_DATE>,  "validTo" : 99991231235959,  "validFrom" : 2017080212000} """.stripMargin.replaceAll("\n", " ")

  val riskType2DList : List[String] = List("Risk1","Risk2","Risk3")
  val riskType1DList : List[String] = List("Risk4","Risk5","Risk6","Risk7","Risk8","Risk9","Risk10","Risk11","Risk12","Risk13")
  val riskTypeScalarList : List[String] = List("Risk14","Risk15","Risk16","Risk17","Risk18","Risk19","Risk20","Risk21","Risk22","Risk23","Risk24","Risk25")
  val r = scala.util.Random



  def get2DSchema( hsbcTradeId : String , tradeId : String, valuationDt : String) : List[String] = {
    var dataListBuffer = new ListBuffer[String]()


    for(riskType <- riskType2DList ){
      val data = {
        schema2D.replaceAll("<RISK_TYPE>", riskType)
          .replaceAll("<HSBC_TRADE_ID>", hsbcTradeId)
          .replaceAll("<TRADE_ID>", tradeId)
          .replaceAll("<VALUATION_DATE>", valuationDt)
          .replaceAll("<MEASURE_VALUE_1>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_2>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_3>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_4>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_5>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_6>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_7>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_8>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_9>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_10>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_11>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_12>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_13>", r.nextFloat().toString)

      }

      dataListBuffer += data
    }
    val dataList = dataListBuffer.toList
    dataList
  }

  def getScalarSchema( hsbcTradeId : String , tradeId : String, valuationDt : String) : List[String] = {
    var dataListBuffer = new ListBuffer[String]()

    for(riskType <- riskTypeScalarList ){
      val data = {
        schemaScalar.replaceAll("<RISK_TYPE>", riskType)
          .replaceAll("<HSBC_TRADE_ID>", hsbcTradeId)
          .replaceAll("<TRADE_ID>", tradeId)
          .replaceAll("<VALUATION_DATE>", valuationDt)
          .replaceAll("<MEASURE_VALUE_1>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_2>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_3>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_4>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_5>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_6>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_7>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_8>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_9>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_10>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_11>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_12>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_13>", r.nextFloat().toString)

      }

      dataListBuffer += data
    }
    val dataList = dataListBuffer.toList
    dataList
  }

  def get1DSchema( hsbcTradeId : String , tradeId : String, valuationDt : String) : List[String] = {
    var dataListBuffer = new ListBuffer[String]()

    for(riskType <- riskType1DList ){
      val data = {
        schema1D.replaceAll("<RISK_TYPE>", riskType)
          .replaceAll("<HSBC_TRADE_ID>", hsbcTradeId)
          .replaceAll("<TRADE_ID>", tradeId)
          .replaceAll("<VALUATION_DATE>", valuationDt)
          .replaceAll("<MEASURE_VALUE_1>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_2>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_3>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_4>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_5>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_6>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_7>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_8>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_9>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_10>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_11>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_12>", r.nextFloat().toString)
          .replaceAll("<MEASURE_VALUE_13>", r.nextFloat().toString)

      }

      dataListBuffer += data
    }
    val dataList = dataListBuffer.toList
    dataList
  }
}

