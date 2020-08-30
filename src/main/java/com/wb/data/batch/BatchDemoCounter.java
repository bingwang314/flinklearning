package com.wb.data.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> res = data.map(new RichMapFunction<String, String>() {
            // 定义累加器
            IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 注册累加器
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        }).setParallelism(4);

        res.writeAsText("H:\\home\\work\\data\\flink\\test2");

        JobExecutionResult jobResult = env.execute("BatchDemoCounter");
        // 获取累加器
        Integer counter = jobResult.getAccumulatorResult("counter");
        System.out.println("counter:" + counter);
    }
}
