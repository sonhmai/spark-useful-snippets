import org.apache.spark.sql.streaming.GroupStateTimeout

withEventTime
    .where("x is not null")
    .selectExpr("user as uid", 
        "cast(Creation_Time/1000000000 as timestamp) as timestamp",
        "x",
        "gt as activity")
    .as[InputRow]
    .withWatermark("timestamp", "5 seconds")
    .groupByKey(_.uid)
    .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
    .writeStream
    .queryName("count_based_device")
    .format("memory")
    .start()

/* Output

+---+--------------------+--------------------+
|uid| activities| xAvg|
+---+--------------------+--------------------+
| a| [stand, null, sit]|-9.10908533566433...|
| a| [sit, null, walk]|-0.00654280428601...|
...
| c|[null, stairsdown...|-0.03286657789999995|
+---+--------------------+--------------------+
*/

case class InputRow(uid:String, timestamp:java.sql.Timestamp, x:Double, activity:String)
case class UserSession(
    val uid:String,
    var timestamp:java.sql.Timestamp,
    var activities:Array[String],
    var values:Array[Double]
)

def updateAcrossEvents(
    uid:String, inputs: Iterator[InputRow], GroupState[UserSession]
): Iterator[UserSessionOutput] = {
    inputs.toSeq.sortBy(_.timestamp.getTime)
        .toIterator
        .flatMap { input => 
            val state = if (oldState.exists) oldState.get else UserSession(
                uid, new java.sql.Timestamp(1L), Array(), Array()
            )
            val newState = updateWithEvent(state, input)
            
        }
}