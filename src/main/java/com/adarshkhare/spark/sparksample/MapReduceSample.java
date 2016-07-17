/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author adkhare\
 *    kk
 */
public class MapReduceSample
{
    public static JavaPairRDD<String, Integer> DoWordCount(JavaSparkContext sc, String inputFile)
    {
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words = input.flatMap((String x) -> Arrays.asList(x.split(" ")));
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair((String x) -> new Tuple2(x, 1));
        counts.reduceByKey((Integer x, Integer y) -> x + y);
        return counts;
    }
}
