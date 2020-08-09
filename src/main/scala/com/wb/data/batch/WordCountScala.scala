package com.wb.data.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCountScala {

  def main(args: Array[String]): Unit = {

    val inputPath = "H:\\home\\work\\data\\data1.txt"
    val outPut = "H:\\home\\work\\data\\result2.txt"


    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.readTextFile(inputPath)

    //引入隐式转换
    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
    counts.writeAsCsv(outPut, "\n", " ").setParallelism(1)
    env.execute("batch word count")
  }
}
