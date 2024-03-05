package Baeldung;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Baeldung {
    public static void main (String[] args ) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

        DataStream <Integer> amount = env.fromElements(1, 29, 40, 50, 70);

        Integer threshold = 30;

        //filter

        DataStream <Integer> filtered = amount.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer amount){
                return amount > threshold;
                }
            });
        
        
        //keyby
        var keyed = filtered.keyBy(value -> 1);

        //reduce


        DataStream <Integer> result = keyed.reduce( new ReduceFunction<Integer>() {
            @Override
            public Integer reduce (Integer value1, Integer value2){
                return value1 + value2;
            }
        } );

        result.print();

        env.execute();

        
    };
}
