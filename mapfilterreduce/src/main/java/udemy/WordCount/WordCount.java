package udemy.WordCount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

    public static void main(String[] args) throws Exception{
        
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream <String> words = env.fromElements("Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Isaac",
     "Julia", "Kevin", "Lena", "Mark", "Nina", "Oliver", "Paul", "Rachel", "Samuel", "Tina", "Victor",
     "Kevin", "Lena", "Mark", "Nina", "Oliver", "Paul", "Rachel", "Samuel", "Tina", "Victor",
     "Frank", "Grace", "Hannah", "Isaac",
     "Julia", "Kevin", "Lena", "Mark", "Nina", "Oliver", "Paul", "Rachel");

     DataStream <Tuple2<String, Integer >> mapped = words.map(new Tokenizer());

     DataStream <Tuple2<String, Integer >> reduced = mapped.keyBy(t->t.f0).reduce(new Reducer());

     reduced.print();
     env.execute();

    }

public static class Reducer implements ReduceFunction <Tuple2<String, Integer >> {
    @Override
    public Tuple2<String, Integer > reduce (Tuple2<String, Integer > current,
    Tuple2<String, Integer > next){
        return new Tuple2<String,Integer>(current.f0, current.f1+next.f1);
     }   
}




public static class Tokenizer implements MapFunction <String, Tuple2<String, Integer >> {
    @Override
    public Tuple2<String, Integer > map (String word){
        return new Tuple2<String, Integer>(word, 1);
    }
}

}