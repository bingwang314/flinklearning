package com.wb.data.batch

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration


object BatchDemoDisCacheScala {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    env.registerCachedFile("H:\\home\\work\\data\\test.txt", "test");

    import org.apache.flink.api.scala._
    val data: DataSet[String] = env.fromElements("a", "b", "c", "d")

    data.map(new RichMapFunction[String, String] {
      var lines: util.List[String] = _;
      override def open(parameters: Configuration) = {
        super.open(parameters)

        // 获取文件数据
        val file: File = getRuntimeContext.getDistributedCache.getFile("test")
        lines = FileUtils.readLines(file)
      }

      override def map(value: String) = {
        Thread.currentThread().getName + ":" + value + ":" + lines.get(0)
      }
    }).setParallelism(2).print()
  }

}
