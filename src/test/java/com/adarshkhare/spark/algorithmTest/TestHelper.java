/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithmTest;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

/**
 *
 * @author adkhare
 */
public class TestHelper
{

    /**
     *
     * @param appName
     * @return
     */
    public static SparkConf InitializeSparkConf(String appName)
    {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.setMaster("local");
        //Override the logging levels
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Logger.getLogger("Remoting").setLevel(Level.ERROR);
        return conf;
    }
    
}
