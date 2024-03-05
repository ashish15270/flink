package flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Baeldung {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> amounts = env.fromElements(1, 29, 40, 50, 70);

        int threshold = 30;
        amounts
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer val) throws Exception {
                        return val > threshold;
                    }
                })
                .map(new MapFunction<Integer, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(Integer value) throws Exception {
                        return new Tuple2<>(1, (long) value);
                    }
                })
                .keyBy(value -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> t1, Tuple2<Integer, Long> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                })
                .print();

        env.execute();
    }
}