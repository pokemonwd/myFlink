package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink08_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取字母流
        DataStreamSource<String> strStreamSource = env.fromElements("a", "b", "c", "d", "e");

        //获取数字流
        DataStreamSource<Integer> intStreamSource = env.fromElements(1, 2, 3, 4);


        //TODO 3.使用Connect链接两条流
        ConnectedStreams<String, Integer> connect = strStreamSource.connect(intStreamSource);

        //4.对链接后的流做map操作
        connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "cls";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 1000 + "";
            }
        }).print();

        env.execute();
    }
}
