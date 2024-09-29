package com.pratice.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.List;

public class WordCountTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("SparkAndJava").setMaster("local[*]");

    @Test
    @DisplayName("Test loading CSV file in RDD")
    public void testLoadingCSVFileInRDD() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            String txtFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            JavaRDD<String> filteredWords =
                    javaSparkContext.textFile(txtFilePath)
                            .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                            .flatMap(line -> List.of(line.split("\\s")).iterator())
                            .filter(word -> (word != null) && (!word.trim().isEmpty()));

            JavaPairRDD<String, Long> pairRDD = filteredWords
                    .mapToPair(word -> new Tuple2<>(word, 1L))
                    .reduceByKey(Long::sum);

            pairRDD.take(10).forEach(System.out::println);
            System.out.println("------------------------------");

            pairRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false).take(10).forEach(System.out::println);
        }
    }

}
