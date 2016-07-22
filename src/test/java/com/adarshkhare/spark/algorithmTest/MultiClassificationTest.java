/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithmTest;

import com.adarshkhare.spark.algorithm.MultiClassification;
import org.junit.*;
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
        System.out.println("DoMultiClassClassification");
        MultiClassification instance = new MultiClassification();
        try
        {
            int numIterations = 10;
            String dataPath = "sample/data/sample_libsvm_data.txt";
            String modelDir = "testModel";
            instance.SplitTestAndTrainingData(0.2, dataPath);
            String savePath = instance.DoMultiClassClassification(numIterations, modelDir);
            assertNotNull(savePath);
            instance.PrintEvaluationMetrics(savePath);
        } 
        finally
        {
            instance.ShutDown();
        }
    }
}
