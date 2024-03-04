package com.flink;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class MapFilterReduce 
{
    public static void main( String[] args )
    {
        List <Integer> myList = new ArrayList<Integer>();
        myList.add(1);
        myList.add(5);
        myList.add(8);

        Stream <Integer> intStream = myList.stream();

       /*Once you have a Stream object, you can use a variety of methods to transform it
        into another Stream object. The first such method we’re going to look at is the map method.
         It takes a lambda expression as its only argument, and uses it to change every individual
          element in the stream. Its return value is a new Stream object containing the changed elements.
 */

 // convert all items in an array to upper string
    String [] names = {"Jon","Tom","Kat","Ron"};

    Stream<String> stringStream =  Arrays.stream(names);
    
    Stream <String> newStream =  stringStream.map(s -> s.toUpperCase());

    String [] myNewArray =  newStream.toArray(String[]::new);

    /*Just like the map method, the filter method expects a lambda expression as
     its argument. However, the lambda expression passed to it must always return a
      boolean value, which determines whether or not the processed element should belong
       to the resulting Stream object. */

    String [] stringLength = {"method", "the" ,"filter","method", "expects"};

    Stream <String> lenStream = Arrays.stream(stringLength);

    Stream <String> filStream = lenStream.filter(s -> s.length() > 3 );

    String [] lenArray = filStream.toArray(String[]::new);

    String [] conciseLenArray = Arrays.stream(stringLength)
      .filter(s -> s.length() > 4)
      .toArray(String[]::new);

    /*A reduction operation is one which allows you to compute a result using all the 
    elements present in a stream. Reduction operations are also called terminal operations
     because they are always present at the end of a chain of Stream methods. We’ve already 
     been using a reduction method in our previous examples: the toArray method. 
    It’s a terminal operation because it converts a Stream object into an array 
    Java 8 includes several reduction methods, such as sum, average and count, which allow 
    to perform arithmetic operations on Stream objects and get numbers as results.*/

    Integer [] ints = {2,4,5,7};

    int result1 = Arrays.stream(ints).filter(i -> i > 4).reduce(1, (a,b) -> a*b);

    int result2 = Arrays.stream(ints).filter(i -> i > 4).reduce(2, (a,b) -> a*b);

    int result3 = Arrays.stream(ints).filter(i -> i > 4).reduce(3, (a,b) -> a*b);

    System.out.println( Integer.toString(result1) + " "+ Integer.toString(result2) +
     " "+ Integer.toString(result3) + " ");

    String [] strArray = {"this", "is", "a", "sentence"};

    String result = Arrays.stream(strArray).reduce("",(a,b) -> a+b);

    System.out.println(result);


    }
}
