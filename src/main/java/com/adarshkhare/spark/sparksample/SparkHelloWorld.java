/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
        SparkConf conf = InitializeSpark();
        try
        {
            String selection = SparkHelloWorld.waitForEnterKey("Select Sample 1. Map Reduce, 2. Multi Class Classifier.");
            switch (selection)
            {
                case "1":
                    TryMapReduceSample(conf);
                    break;
                case "2":
                    TryMultiClassClassifierSample(conf);
                    break;
                default:
                    TryMultiClassClassifierSample(conf);
                    break;

            }

        } finally
        {
            SparkHelloWorld.waitForEnterKey("Press <Enter> to teminate the program.");
        }
    }

    private static void TryMultiClassClassifierSample(SparkConf conf)
    {

        String path = "sample/data/sample_libsvm_data.txt";
        MultiClassificationSample classifier = new MultiClassificationSample(new SparkContext(conf));
        JavaRDD<Tuple2<Object, Object>> result = classifier.DoMultiClassClassification(path);
        SparkHelloWorld.waitForEnterKey("Press <Enter> to print evaluation metrics.");
        classifier.PrintEvaluationMetrics(result);
    }

    private static void TryMapReduceSample(SparkConf conf)
    {
        String inputFile = "sample/data/mapreduce_data.txt";
        List<Tuple2<String, Integer>> counts = MapReduceSample.DoWordCount(new JavaSparkContext(conf), inputFile);
        counts.forEach((result) ->
        {
            System.out.println(result._1 + "=" + result._2);
        }); //or pairRdd.collect()
    }

    private static SparkConf InitializeSpark()
    {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("Samples");
        conf.setMaster("local");
        //Override the logging levels
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        return conf;
    }

    private static String waitForEnterKey(String promptMessage)
    {
        try
        {
            System.out.print(promptMessage);
            BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
            return buffer.readLine();
        } 
        catch (IOException ex)
        {
            Logger.getLogger(SparkHelloWorld.class.getName()).log(Level.FATAL, null, ex);
        }
        return StringUtils.EMPTY;
    }
}
