package com.wb.data.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoHashRangePartitionScala {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data1: ListBuffer[(Int, String)] = ListBuffer((1, "hello1"),
      (2, "hello2"),
      (2, "hello3"),
      (1, "hello1"),
      (2, "hello2"),
      (2, "hello3"),
      (3, "hello4"),
      (3, "hello5"),
      (3, "hello6"),
      (4, "hello7"),
      (4, "hello8"),
      (4, "hello9"),
      (4, "hello10"),
      (5, "hello11"),
      (5, "hello12"),
      (5, "hello13"),
      (5, "hello14"),
      (5, "hello15"),
      (6, "hello16"),
      (6, "hello17"),
      (6, "hello18"),
      (6, "hello19"),
      (6, "hello20"),
      (6, "hello21"))

    val text: DataSet[(Int, String)] = env.fromCollection(data1)
    text.partitionByRange(0).mapPartition(it => {
      while (it.hasNext) {
        val tuple: (Int, String) = it.next()
        println("当前线程id：" + Thread.currentThread().getId + "," + tuple)
      }
      it
    }).print()

    println("===========================================================")
    text.map(t => {
      println("当前线程id：" + Thread.currentThread().getId + "," + t)
      (Thread.currentThread().getId + ":" + t._1, t._2)
    }).print()


  }
}
