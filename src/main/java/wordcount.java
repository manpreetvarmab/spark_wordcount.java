import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class wordcount {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("sample").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<String> rdd1 = jsc.textFile("C:/Users/Manpreet Varma/Desktop/tutorials.txt");
        JavaRDD<String> rdd2= rdd1.flatMap(x-> Arrays.asList(x.split(" ")));
        JavaPairRDD<String, Integer> count =rdd2.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
        JavaPairRDD<String, Integer> wordcount=count.reduceByKey((x, y) -> x + y);
        System.out.println(wordcount.collect());
    }

}