package com.wb.data.wikiedits


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}


object WikipediaAnalysisScala {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val edits: DataStream[WikipediaEditEvent] = environment.addSource(new WikipediaEditsSource)
    edits.keyBy(_.getUser)
      .timeWindow(Time.seconds(5))
      .fold(("", 0L))((init, editEvent) => (editEvent.getUser, editEvent.getByteDiff))
      .map(_.toString())
      .addSink(new FlinkKafkaProducer011("localhost:9092", "wiki-result", new SimpleStringSchema))

    environment.execute()
  }
}
