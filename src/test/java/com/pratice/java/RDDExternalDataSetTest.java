package com.pratice.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

public class RDDExternalDataSetTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("SparkAndJava").setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/1000words.txt",
            "src/test/resources/wordslist.txt.gz"
    })
    @DisplayName("Test Loading Local Text File In RDD")
    public void testLoadingLocalTextFileInRDD(final String localFilePath) {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> javaRDD = javaSparkContext.textFile(localFilePath);
            System.out.printf("Total lines in the file: %d%n ", javaRDD.count());
            System.out.println("Printing first 10 lines::::");
            javaRDD.take(10).forEach(System.out::println);

            System.out.println("------------------------------");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test Loading Local Text File In RDD using method source")
    public void testLoadingLocalTextFileInRDDUsingMethodSource(final String localFilePath) {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> javaRDD = javaSparkContext.textFile(localFilePath);
            System.out.printf("Total lines in the file: %d%n ", javaRDD.count());
            System.out.println("Printing first 10 lines::::");
            javaRDD.take(10).forEach(System.out::println);

            System.out.println("------------------------------");
        }
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString())
        );
    }

    @Test
    @DisplayName("Test loading whole directory in RDD")
    public void testLoadingWholeDirectoryInRDD() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            String testDirectoryPath = Path.of("src", "test", "resources").toString();
            JavaPairRDD<String, String> javaRDD = javaSparkContext.wholeTextFiles(testDirectoryPath);
            System.out.printf("Total files in directory: %d%n ", javaRDD.count());
            javaRDD.collect()
                    .forEach(tuple -> {
                        System.out.printf("file name: %s%n", tuple._1);
                        System.out.println("------------------------------");
                        if(tuple._1.endsWith("properties")) {
                            System.out.println(tuple._2);
                        }
            });
        }
    }

    @Test
    @DisplayName("Test loading CSV file in RDD")
    public void testLoadingCSVFileInRDD() {
        try(JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            String csvFilePath = Path.of("src", "test", "resources", "dma.csv").toString();
            JavaRDD<String> javaRDD = javaSparkContext.textFile(csvFilePath);
            System.out.printf("Total lines in csv: %d%n ", javaRDD.count());
            javaRDD.take(10).forEach(System.out::println);
        }
    }

}
