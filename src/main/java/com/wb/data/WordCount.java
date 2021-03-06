package com.wb.data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env =ExecutionEnvironment.getExecutionEnvironment();



        DataSet<String> text = env.fromElements(

                "hadoop hive?",

                "think hadoop hive sqoop hbase spark flink?");



        DataSet<Tuple2<String,Integer>> wordCounts = text

                .flatMap(new FlatMapFunction<String,Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String,Integer>> out) throws Exception {
                        for (String word : line.split("\\W+")) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })

                .groupBy(0)

                .sum(1);

        wordCounts.print();
    }

}


