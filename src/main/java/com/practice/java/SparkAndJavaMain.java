package com.practice.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SparkAndJavaMain {

    public static void main(String[] args) {
        try(SparkSession sparkSession = SparkSession.builder()
                .appName("SparkAndJava").master("local[*]")
                .getOrCreate();
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext())
        )
        {
            List<Integer> list = IntStream.rangeClosed(1, 16).boxed().collect(Collectors.toList());
            list.forEach(System.out::println);
            JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);
            System.out.printf("Total elements in RDD: %d%n", javaRDD.count());
            System.out.printf("Default number of partitions in RDD: %d%n", javaRDD.getNumPartitions());
            Integer max = javaRDD.reduce(Integer::max);
            Integer min = javaRDD.reduce(Integer::min);
            Integer sum = javaRDD.reduce(Integer::sum);
            System.out.printf("From the RDD, Max: %d, Min: %d, Sum: %d%n", max, min, sum);

            // to stop the main program and check Spark UI localhost:4040
            /*try(Scanner sc = new Scanner(System.in);) {
                sc.nextLine();
            }*/
        }
    }

}