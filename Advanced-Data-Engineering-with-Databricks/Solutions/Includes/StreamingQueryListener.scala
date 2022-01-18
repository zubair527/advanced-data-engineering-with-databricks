// Databricks notebook source
// MAGIC %md
// MAGIC This script provides a custom Streaming Query Listener that log progress as JSON files. [Docs](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#reporting-metrics-programmatically-using-asynchronous-apis)
// MAGIC 
// MAGIC To reset the target logging directory, change the cell to `%run ../Includes/StreamingQueryListener $reset="true"`

// COMMAND ----------

val username = spark.sql("SELECT current_user()").first.getString(0)

dbutils.widgets.text("reset", "false")
val reset = dbutils.widgets.get("reset")
if(reset=="true") {
  dbutils.fs.rm(s"dbfs:/user/$username/streaming_logs/", true)
}

// COMMAND ----------

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming._

import java.io.File

class customListener() extends StreamingQueryListener {

  // Sink
  private val fileDirectory = s"/dbfs/user/$username/streaming_logs/"

  // Modify StreamingQueryListener Methods 
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
  }

  // Send Query Progress metrics to DBFS 
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val file = new File(
      fileDirectory + event.progress.name + event.progress.id + "_" + event.progress.batchId + ".json"
    )
    FileUtils.touch(file)
    FileUtils.writeStringToFile(file, event.progress.json)
  }
}

val streamingListener = new customListener()

spark.streams.addListener(streamingListener)

