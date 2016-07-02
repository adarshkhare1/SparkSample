/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class SparkHelloWorld
{

    public static void main(String[] args) throws Exception
    {

    

        String inputFile = "c:\\readme.txt";
        String outputFile = "c:\\result.txt";

// Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

// Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

// Split up into words.
        JavaRDD<String> words = input.flatMap((String x) -> Arrays.asList(x.split(" ")));

// Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair((String x) -> new Tuple2(x, 1));
        counts.reduceByKey((Integer x, Integer y) -> x + y);

// Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(outputFile);

    }

}
