spark.conf.set("spark.sql.shuffle.partitions", 5)
val static = spark.read.json("/data/activity-data")
val streaming = spark
    .readStream
    .schema(static.schema)
    .option("maxFilesPerTrigger", 10)
    .json("/data/activity-data")

// convert timestamp col in Long (unix) into proper Spark SQL timestamp type
val withEventTime = streaming.selectExpr(
    "*",
    "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time"
)

// tumbling window ================
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
    .writeStream
    .queryName("events_per_window")
    .format("memory")
    .outputMode("complete")
    .start()

import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes"), "User").count()
    .writeStream
    .queryName("events_per_window")
    .format("memory")
    .outputMode("complete")
    .start()

// sliding window ============
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
    .count()
    .writeStream
    .queryName("events_per_window")
    .format("memory")
    .outputMode("complete")
    .start()

// watermarking =============
import org.apache.spark.sql.functions.{window, col}
withEventTime
    .withWatermark("event_time", "30 minutes")
    .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
    .count()
    .writeStream
    .queryName("events_per_window")
    .format("memory")
    .outputMode("complete")
    .start()

// dedup ====================
withEventTime
    .withWatermark("event_time", "5 seconds")
    .dropDuplicates("User", "event_time")
    .groupBy("User")
    .count()
    .writeStream
    .queryName("deduplicated")
    .format("memory")
    .outputMode("complete")
    .start()

