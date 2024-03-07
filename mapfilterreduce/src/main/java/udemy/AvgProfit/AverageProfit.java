package udemy.AvgProfit;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {

    public static void main(String[] args) {
        
    
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat()).build();

    final DataStream<String> data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "avg");

    //01-06-2018,June,Category5,Bat,12  -> month, product, category, profit, count
    //1. split

    DataStream <Tuple5<String,String,String,Integer,Integer>> mapped = data.map(new Splitter() );

    //reduce  function to add profit and counts

    DataStream <Tuple5<String,String,String,Integer,Integer>> reduced = mapped.
    keyBy(t->t.f3).reduce(new Reducer());

    // calculate average profit = sum(profit)/counts
    // <Tuple5<String,String,String,Integer,Integer>> -> month, avg. profit

    /*DataStream < Tuple2 < String, Double >> profitPerMonth =
      reduced.map(new MapFunction < Tuple5 < String, String, String, Integer, Integer > , Tuple2 < String, Double >> () {
        public Tuple2 < String, Double > map(Tuple5 < String, String, String, Integer, Integer > input) {
          return new Tuple2 < String, Double > (input.f0, new Double((input.f3 * 1.0) / input.f4));
        }
      }); 

    DataStream <Tuple2<String, Integer>> result = reduced.map(new MapFunction 
    <Tuple5<String,String,String,Integer,Integer>,Tuple2 <String, Integer>> () {
        @Override
        public Tuple2 <String, Double> map(Tuple5<String,String,String,Integer,Integer> profit){
            return new Tuple2<String, Double>(profit.f1,profit.f3.profit.f4);
        }
        }); */

        SingleOutputStreamOperator result = reduced.map(new Mapper());

        result.print();
        env.execute();



} 

public static class Splitter implements MapFunction <String , Tuple5 <String,String,String,Integer,Integer>>{
    @Override
    public Tuple5 <String,String,String,Integer,Integer> map(String value){
        String [] words = value.split(",");

        return new Tuple5<String ,String ,String ,Integer,Integer >
         (words[1], words[2], words[3],Integer.parseInt(words[4]),1);

    } 
}

// reduce function takes and returns same datatype
public static class Reducer implements ReduceFunction <Tuple5 <String,String,String,Integer,Integer>>{
    @Override 
    public Tuple5 <String,String,String,Integer,Integer> reduce ( Tuple5 <String,String,String,Integer,Integer> current ,
    Tuple5 <String,String,String,Integer,Integer> next){
        return new Tuple5 < String, String, String, Integer, Integer >
        (current.f0, current.f1,
        current.f2, current.f3+next.f3, current.f4 + next.f4);
    }
}

public static class Mapper implements MapFunction
 < Tuple5 < String, String, String, Integer, Integer >, Tuple2<String,Double> > {
    @Override
    public Tuple2 <String, Double> map(Tuple5<String,String,String,Integer,Integer> profit){
        return new Tuple2 <String, Double> (profit.f1,profit.f3*1.0/profit.f4);  
 }


 }



}


