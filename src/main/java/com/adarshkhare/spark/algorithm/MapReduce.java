/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithm;

import com.adarshkhare.spark.datapipeline.email.VocabularyBuilder;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class MapReduce
{

    private static final String MESSAGE_FILTER = "(From:.*"
            + "|Sent:.*"
            + "|To:.*"
            + "|Subject:.*"
            + "|ScratchShipmentGroupId.*"
            + "|https:.*"
            + "|Message:.*)";

    /**
     *
     * @param inputFile
     * @return
     */
    public static List<Tuple2<String, Integer>> DoWordCount(String inputFile)
    {
        SparkConf conf = SparkInitalizer.InitializeSparkConf(MapReduce.class.getName());
        JavaSparkContext spark = new JavaSparkContext(conf);
        try
        {
            // Load our input data.
            JavaRDD<String> input;
            input = spark.textFile(inputFile).filter((String v1)
                    -> 
                    {
                        return !v1.trim().matches(MESSAGE_FILTER);
            }
            );

            JavaRDD<String> words;
            words = input.flatMap((String x) ->
            {
                String normalizedLine = x.trim().toLowerCase();
                if (VocabularyBuilder.KNOWN_MESSAGES.contains(normalizedLine))
                {
                    return Arrays.asList(normalizedLine).iterator();
                }
                else
                {
                    return Arrays.asList(normalizedLine.split("[\\p{Punct}\\s]+")).iterator();
                }
            });
            // Transform into word and count.
            JavaPairRDD<String, Integer> counts = words.mapToPair((String x) -> new Tuple2(x, 1));
            counts = counts.reduceByKey((Integer x, Integer y) -> x + y);
            return counts.collect();
        }
        finally
        {
            spark.stop();
        }
    }
}
