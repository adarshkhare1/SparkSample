/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithmTest;

import com.adarshkhare.spark.algorithm.MapReduce;
import java.util.ArrayList;
import java.util.List;
import org.junit.*;
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
        System.out.println("DoWordCount");
        String inputFile = "sample/data/mapreduce_data.txt";
        List<Tuple2<String, Integer>> expResult = new ArrayList<>();
        expResult.add(new Tuple2<>("adarsh", 4));
        expResult.add(new Tuple2<>("khare", 4));
        List<Tuple2<String, Integer>> result = MapReduce.DoWordCount(inputFile);
        assertEquals(expResult, result);
    }
}
