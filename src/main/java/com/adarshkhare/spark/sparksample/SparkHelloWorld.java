/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
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

        String inputFile = "c:\\readme.txt";
        JavaPairRDD<String, Integer> counts = MapReduceSample.DoWordCount(new JavaSparkContext(conf), inputFile);
        String outputFile = "c:\\result.txt";
        counts.saveAsTextFile(outputFile);
        
        MultiClassificationSample.DoMultiClassClassification(new SparkContext(conf));

    }

    private static SparkConf InitializeSpark()
    {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("Samples");
        conf.setMaster("local");
        return conf;
    }
    
    

}
