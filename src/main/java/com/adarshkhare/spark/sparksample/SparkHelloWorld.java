/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import com.adarshkhare.spark.algorithm.MapReduce;
import com.adarshkhare.spark.algorithm.MultiClassification;
import com.adarshkhare.spark.datapipeline.email.VocabularyBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
        SparkConf conf = InitializeSparkConf();
        try
        {
            String selection = SparkHelloWorld.waitForEnterKey("Select Sample 1.eMailVocab builder");
            switch (selection)
            {
                default:
                    SparkHelloWorld.PopulateVocabulary(conf);
                    break;

            }

        } finally
        {
            SparkHelloWorld.waitForEnterKey("Press <Enter> to teminate the program.");
        }
    }
    
    private static void PopulateVocabulary(SparkConf conf)
    { 
        JavaSparkContext spark = new JavaSparkContext(conf);
        try
        {
            String inputFile = "/Adarsh/eMailData/eMailSamples/1.txt";
            VocabularyBuilder vb = new VocabularyBuilder();
            List<Tuple2<String, Integer>> counts = MapReduce.DoWordCount(spark, inputFile);
            counts.forEach((result)
                    -> 
                    {
                        vb.addWordInVocabulary(result._1);
            });
            vb.SaveVocabulary();
        }
        finally
        {
            spark.stop();
        }
    }

    private static SparkConf InitializeSparkConf()
    {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("Samples");
        conf.setMaster("local");
        //Override the logging levels
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Logger.getLogger("Remoting").setLevel(Level.ERROR);
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
            Logger.getLogger("SparkSample").log(Level.FATAL, null, ex);
        }
        return StringUtils.EMPTY;
    }
}
