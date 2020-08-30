package com.wb.data.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class BatchDemoCross {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data1 = env.fromElements("a", "b", "c");
        DataSource<Integer> data2 = env.fromElements(1, 2, 3);

        System.out.println("=======================================");
        data1.print();
        System.out.println("=======================================");
        data2.print();
        System.out.println("=======================================");
        data1.cross(data2).print();
    }
}
