package com.wb.data.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class BatchDemoFirst {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> list = new LinkedList<>();
        list.add(new Tuple2<>(1, "zs"));
        list.add(new Tuple2<>(1, "ls"));
        list.add(new Tuple2<>(1, "ww"));
        list.add(new Tuple2<>(1, "zl"));
        list.add(new Tuple2<>(2, "zs"));
        list.add(new Tuple2<>(2, "ls"));
        list.add(new Tuple2<>(2, "zl"));
        list.add(new Tuple2<>(3, "zl"));
        list.add(new Tuple2<>(2, "zl"));
        list.add(new Tuple2<>(3, "xm"));
        list.add(new Tuple2<>(3, "wm"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(list);

        System.out.println("==================================");
        data.first(3).print();
        System.out.println("==================================");
        data.groupBy(0).first(2).print();
        System.out.println("==================================");
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
    }
}
