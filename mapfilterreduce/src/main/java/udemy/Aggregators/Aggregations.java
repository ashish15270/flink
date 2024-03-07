package udemy.Aggregators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregations {

    public static void main(String[] args) throws Exception{
        
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat()).build();

    final DataStream<String> data = env.fromSource(source, WatermarkStrategy.noWatermarks(),
     "/home/ubuntu/udemy/flink/mapfilterreduce/src/main/java/udemy/Aggregators/avg1");

    DataStream <Tuple5<String,String,String,Integer,Integer>> splitted = data.map(new Splitter());
    //     [June,Category4,Perfume,10,1]

    //sum

    splitted.keyBy(t->t.f0).sum(3).print();

    //min
    splitted.keyBy(t->t.f0).min(3).print();

    //max
    splitted.keyBy(t->t.f0).max(3).print();

    //minby
    splitted.keyBy(t->t.f0).minBy(3).print();

    //maxby
    splitted.keyBy(t->t.f0).maxBy(3).print();

    env.execute();

    }

    public static class Splitter implements MapFunction <String, Tuple5<String,String,String,Integer,Integer>> {
        @Override
        public Tuple5<String,String,String,Integer,Integer> map (String values){
            String [] value = values.split(",");
            return new Tuple5 <String,String,String,Integer,Integer> 
            (value[1],value[2],value[3],Integer.parseInt(value[4]),1);
        }
    }
    
}
