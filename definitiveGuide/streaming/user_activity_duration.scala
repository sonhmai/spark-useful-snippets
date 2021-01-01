// user defined stateful processing ========================
/**
- Map over groups in your data, operate on each group of data, and generate at most a
single row for each group. The relevant API for this use case is mapGroupsWithState.
- Map over groups in your data, operate on each group of data, and generate one or more
rows for each group. The relevant API for this use case is flatMapGroupsWithState.
*/
case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
case class UserState(
    user:String, var activity:String,
    var start:java.sql.Timestamp,
    var end:java.sql.Timestamp
)

def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
    if (Option(input.timestamp).isEmpty) return state
    if (state.activity == input.activity) {
        if (input.timestamp.after(state.timestamp)) state.end = input.timestamp
        else if (input.timestamp.before(state.start)) state.start = input.timestamp
    }
    // why need to specify case activity not matched?
}
// how state is updated based on epoch of rows
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
// GroupState created separately for every aggregation key to hold a state
// as an aggregation state value
def updateAcrossEvents(
    user:String, inputs:Iterator[InputRow], oldState: GroupState[UserState]
): UserState = {
    var state: UserState = if(oldState.exists) oldState.get else UserState(
        user, "", new java.sql.Timestamp(0L), new java.sql.Timestamp(0L)
    )
    for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
    }
    state
}

import org.apache.spark.sql.streaming.GroupStateTimeout
withEventTime
    .selectExpr("User as user",
        "cast(Creation_Time/1000000000 as timestamp) as timestamp", 
        "gt as activity")
    .as[InputRow]
    .groupByKey(_.user)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
    .writeStream
    .queryName("events_per_window")
    .format("memory")
    .outputMode("update")
    .start()
