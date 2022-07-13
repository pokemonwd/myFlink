package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        //如果在读取hdfs的时候出现权限不足,可以通过这种方式指定读取的用户,默认使用的是当前环境的用户,比如windows中的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        //TODO 2.从文件中获取数据
        //DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/words.txt");

        streamSource.print();

        env.execute();
    }
}
