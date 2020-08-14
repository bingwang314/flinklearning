package com.wb.data.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoFirstNScala {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val data: ListBuffer[(Int, String)] = ListBuffer[(Int, String)]()

    data.append((1, "zs"));
    data.append((1, "ls"));
    data.append((1, "ww"));
    data.append((2, "aa"));
    data.append((2, "ab"));
    data.append((3, "cc"));

    import org.apache.flink.api.scala._
    val text: DataSet[(Int, String)] = env.fromCollection(data)

    //获取前3条数据，按照插入顺序
    text.first(3).print()
    println("=====================================================")

    //根据数据第一列进行分组，获取每组的前2个元素
    text.groupBy(0).first(2).print()
    println("=====================================================")

    //根据第一列分组，根据第二列进行组内升序排序，取组内前2个元素
    text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
    println("=====================================================")

  }

}
