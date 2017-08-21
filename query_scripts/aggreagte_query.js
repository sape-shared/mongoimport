conn = new Mongo('localhost:27017');
db = conn.getDB('risk');
var snapShotTime = NumberLong("20170111130000");
  var startTime = new Date();
   var op = db.sampleTrade.aggregate([{ $match : {"index.bookId": {$in: ["0100","0101","0200","0250"]}}},
                           {$project : {"tradeId" : "$index.tradeId", "bookId" : "$index.bookId"}},
                           {
                            $lookup:
                                         {
                                                         from : "measurenested4",
                                                         localField: "tradeId",
                                                         foreignField: "riskSource.tradeId",
                                                         as : "risk_data"
                                         }
                            },
                            {
                              $match: { "risk_data" : { $ne : []}}
                            },
							{
								$project: {"risk_data" : "$risk_data", _id:0}
							},
							{
								$unwind : "$risk_data"
							},
							{
								$match : {"risk_data.validTo" : { $gte : snapShotTime}, "risk_data.validFrom" : {$lte : snapShotTime}}
							}
                         ])

 op.forEach(printjson)
 var endTime = new Date();
print(endTime - startTime);
