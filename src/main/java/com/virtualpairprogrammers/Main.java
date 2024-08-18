package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> myRDDFlat = initialRDD
                .map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap(value-> Arrays.asList(value.split(" ")).iterator())
                .filter(value -> Util.isNotBoring(value) && !value.trim().isEmpty());

//        List<String> take50  = myRDDFlat.take(50);
//        take50.forEach(value->System.out.println(value));

        JavaPairRDD<String, Long> wordCount = myRDDFlat
                        .mapToPair(value -> new Tuple2<>(value, 1L))
                        .reduceByKey(Long::sum);

        JavaPairRDD<Long, String> swap = wordCount
                .mapToPair(value -> new Tuple2<>(value._2, value._1))
                .sortByKey(false);

        List<Tuple2<Long, String>> take50  = swap.take(50);
        take50.forEach(value->System.out.println(value));

        sc.close();
    }
}
