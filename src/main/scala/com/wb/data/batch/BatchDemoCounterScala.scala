package com.wb.data.batch

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

/**
 * counter 累加器
 */
object BatchDemoCounterScala {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data: DataSet[String] = env.fromElements("a", "b", "c", "d")

    val res: DataSet[String] = data.map(new RichMapFunction[String, String] {
      // 定义累加器
      private val counter: IntCounter = new IntCounter

      override def open(parameters: Configuration) = {
        super.open(parameters)
        // 注册累加器
        getRuntimeContext.addAccumulator("counter", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    }).setParallelism(2)

    // 数据持久化
    res.writeAsText("H:\\home\\work\\data\\flink\\test1")

    val jobResult: JobExecutionResult = env.execute("BatchDemoCounterScala")
    // 获取累加器
    val num: Int = jobResult.getAccumulatorResult[Int]("counter")
    println(s"num:$num")
    println(s"data num:" + res.count())
  }

}
