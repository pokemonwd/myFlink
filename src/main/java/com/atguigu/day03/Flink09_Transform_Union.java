package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取三条流
        DataStreamSource<String> source1 = env.fromElements("a", "b", "c", "d");
        DataStreamSource<String> source2 = env.fromElements("1", "2", "3", "4");
        DataStreamSource<String> source3 = env.fromElements("A", "B", "C", "D");

        //TODO 3.使用union连接多条流
        DataStream<String> union = source1.union(source2, source3);

        SingleOutputStreamOperator<String> map = union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "gogogo";
            }
        });

        map.print();

        env.execute();
    }
}
