package com.wb.data.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * 笛卡尔积
 */
object BatchDemoCrossScala {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data1: DataSet[Int] = env.fromElements(1, 2)
    val data2: DataSet[String] = env.fromElements("zs", "ls")

    println("=========================================")
    data1.print()
    println("=========================================")
    data2.print()
    println("=========================================")
    val result: CrossDataSet[Int, String] = data1.cross(data2)
    result.print()
  }
}
