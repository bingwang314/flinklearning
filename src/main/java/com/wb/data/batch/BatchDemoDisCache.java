package com.wb.data.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class BatchDemoDisCache {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("H:\\home\\work\\data\\test.txt", "test");

        DataSource<Integer> data = env.fromElements(1, 2, 3, 4, 5);

        data.map(new RichMapFunction<Integer, String>() {
            List<String> list;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 获取文件
                File file = getRuntimeContext().getDistributedCache().getFile("test");
                list = FileUtils.readLines(file);
            }

            @Override
            public String map(Integer value) throws Exception {
                return Thread.currentThread().getName() + ":" + value + ":" + list.get(0);
            }
        }).setParallelism(3).print();


    }
}
