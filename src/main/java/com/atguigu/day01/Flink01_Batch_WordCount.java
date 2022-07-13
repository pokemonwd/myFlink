package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        /**
         * 1.创建sparkConf
         * 2.创建sparkContext
         * 3.读取文件中的数据(textfile) => RDD(弹性分布式数据集)
         * 4.使用flatMap算子将文件中的每一行数据按照空格切分,切出每一个单词 .flatMap(_.split(" "))
         * 5.使用map并组成Tuple2元组 .map((_,1)))
         * 6.使用reduceByKey(1.将相同的单词聚合到一块 2.做累加) .reduceByKey(_+_)
         * 7.打印结果
         * 8.关闭spark连接
         */

        //1.创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.获取文件中的数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        //3.将文件中的每一行数据按照空格切分,切出每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMap());

        //4.将每一个单词组成Tuple2元组
        MapOperator<String, Tuple2<String, Integer>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
//                return new Tuple2<>(value,1);
                return Tuple2.of(value, 1);
            }
        });

        //5.将相同的单词聚合到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //6.做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //7.将结果打印到控制台
        result.print();

    }

    public static class MyFlatMap implements FlatMapFunction<String,String>{

        /**
         *
         * @param s 传入的数据
         * @param collector 采集器 可以将数据发送至下游
         * @throws Exception
         */
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            //将数据按照空格切分
            String[] words = s.split(" ");
            //遍历每一个单词
            for (String word : words) {
                collector.collect(word);
            }
        }
    }
















}
