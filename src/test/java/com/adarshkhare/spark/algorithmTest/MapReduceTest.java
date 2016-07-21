/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithmTest;

import com.adarshkhare.spark.algorithm.MapReduce;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class MapReduceTest
{
    
    public MapReduceTest()
    {
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }

    /**
     * Test of DoWordCount method, of class MapReduce.
     */
    @org.junit.Test
    public void testDoWordCount()
    {
        JavaSparkContext spark = new JavaSparkContext(TestHelper.InitializeSparkConf("MapReduceTest"));
        try
        {
            System.out.println("DoWordCount");

            String inputFile = "sample/data/mapreduce_data.txt";
            List<Tuple2<String, Integer>> expResult = new ArrayList<>();
            expResult.add(new Tuple2<>("adarsh", 4));
            expResult.add(new Tuple2<>("khare", 4));
            List<Tuple2<String, Integer>> result = MapReduce.DoWordCount(spark, inputFile);
            assertEquals(expResult, result);
        } finally
        {
            spark.stop();
        }
    }
    
    
}
