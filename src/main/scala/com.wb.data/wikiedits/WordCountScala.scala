package com.wb.data.wikiedits

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = env.fromElements(
    "Who’s there?",
    "I think I hear them. Stand, ho! Who’s there?")

    val wordCounts: AggregateDataSet[(String, Int)] = text.flatMap(_.split("\\W+")).map((_, 1)).groupBy(0).sum(1)
//    val wordCounts: AggregateDataSet[WordWithCount] = text.flatMap(_.split("\\W+"))
//      .map(WordWithCount(_, 1))
//      .groupBy(0)
//      .sum("count")

    wordCounts.print()

  }
}

case class WordWithCount(word: String, count: Int)
