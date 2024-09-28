package com.pratice.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CreateRDDUsingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("SparkAndJava").setMaster("local[*]");

    @Test
    @DisplayName("Create Empty RDD With No Partitions In Spark")
    public void createEmptyRDDWithNoPartitionsInSpark() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> javaRDD = javaSparkContext.emptyRDD();
            System.out.println(javaRDD);
            System.out.printf("No of Partitions: %d%n", javaRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create Empty RDD With Default Partitions In Spark")
    public void createEmptyRDDWithDefaultPartitionsInSpark() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> javaRDD = javaSparkContext.parallelize(List.of());
            System.out.println(javaRDD);
            System.out.printf("No of Partitions: %d%n", javaRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create Spark RDD using parallelize")
    public void createSparkRDDUsingParallelize() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> list = IntStream.rangeClosed(1, 16).boxed().collect(Collectors.toList());
            JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);
            System.out.println(javaRDD);
            System.out.printf("No of partitions: %d%n", javaRDD.getNumPartitions());
            System.out.printf("Total elements in the RDD: %d%n", javaRDD.count());
            System.out.println("Elements in the RDD: ");
            javaRDD.collect().forEach(System.out::println);
            Integer max = javaRDD.reduce(Integer::max);
            Integer min = javaRDD.reduce(Integer::min);
            Integer sum = javaRDD.reduce(Integer::sum);
            System.out.printf("From the RDD, Max: %d, Min: %d, Sum: %d%n", max, min, sum);
        }
    }

    @Test
    @DisplayName("Create Spark RDD using parallelize with given partitions")
    public void createSparkRDDUsingParallelizeWithGivenPartitions() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> list = IntStream.rangeClosed(1, 16).boxed().collect(Collectors.toList());
            JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list, 16);
            System.out.println(javaRDD);
            System.out.printf("No of partitions: %d%n", javaRDD.getNumPartitions());
            System.out.printf("Total elements in the RDD: %d%n", javaRDD.count());
            System.out.println("Elements in the RDD: ");
            javaRDD.collect().forEach(System.out::println);
            Integer max = javaRDD.reduce(Integer::max);
            Integer min = javaRDD.reduce(Integer::min);
            Integer sum = javaRDD.reduce(Integer::sum);
            System.out.printf("From the RDD, Max: %d, Min: %d, Sum: %d%n", max, min, sum);
        }
    }

}
