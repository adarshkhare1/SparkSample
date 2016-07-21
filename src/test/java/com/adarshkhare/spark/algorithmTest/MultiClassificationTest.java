/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithmTest;

import com.adarshkhare.spark.algorithm.MultiClassification;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author adkhare
 */
public class MultiClassificationTest
{
    
    public MultiClassificationTest()
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
     * Test of DoMultiClassClassification method, of class MultiClassification.
     */
    @Test
    public void testDoMultiClassClassification()
    {
        SparkContext spark = new SparkContext(TestHelper.InitializeSparkConf("MultiClassificationTest"));
        System.out.println("DoMultiClassClassification");
        try
        {
            int numIterations = 10;
            String dataPath = "sample/data/sample_libsvm_data.txt";
            String modelDir = "testModel";
            MultiClassification instance = new MultiClassification(spark);
            instance.SplitTestAndTrainingData(0.2, dataPath);
            String savePath = instance.DoMultiClassClassification(numIterations, modelDir);
            assertNotNull(savePath);
            instance.PrintEvaluationMetrics(savePath);
        } 
        finally
        {
            spark.stop();
        }
    }
}
