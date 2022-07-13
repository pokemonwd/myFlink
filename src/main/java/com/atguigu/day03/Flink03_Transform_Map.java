package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //读取文件不一定是有界数据,也有可能是无界的
        DataStreamSource<String> streamSource1 = env.readTextFile("input/sensor.txt");

        DataStreamSource<String> streamSource2 = env.fromElements("s1,1,1", "s2,1,1");

        //TODO 3.使用Map将从端口读出来的字符串转为WaterSensor
//        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
//
//            @Override
//            public WaterSensor map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//            }
//        });

//        SingleOutputStreamOperator<WaterSensor> map = streamSource1.map(new MyMap());

        SingleOutputStreamOperator<WaterSensor> map = streamSource2.map(new MyMap());

        map.print();

        env.execute();

    }

    //TODO 富函数
    public static class MyMap extends RichMapFunction<String,WaterSensor>{
        /**
         * 生命周期方法,最先被调用,预示着程序的开始,每个并行度调用一次
         * 适用场景:做一些初始化操作,比如创建链接
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        /**
         * 生命周期方法,最后被调用,预示着程序的结束,每个并行度调用一次,当从文件读取数据时每个并行度调用两次
         * 适用场景:做一些收尾工作,比如资源的释放,关闭链接
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            //getRuntimeContext方法的作用:可以获取更多信息
            System.out.println("JobId:"+getRuntimeContext().getJobId());
            System.out.println("TaskName:"+getRuntimeContext().getTaskName());
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }
    }



























}
