/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import java.io.File;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class MultiClassificationSample
{
    private SVMModel model;
    private final SparkContext sparkContext;
    JavaRDD<LabeledPoint> testData;
    JavaRDD<LabeledPoint> trainingData;
    
    /**
     *
     * @param context
     */
    public MultiClassificationSample(SparkContext context)
    {
        sparkContext = context;
    }
    
    public void SplitTestAndTrainingData(double percentDataForTraining, String dataPath)
    {
         JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(this.sparkContext, dataPath).toJavaRDD();
        // Split initial RDD into two... [60% training data, 40% testing data].
        trainingData = data.sample(false, percentDataForTraining, 11L);
        trainingData.cache();
        testData = data.subtract(trainingData);
        testData.cache();
    }
    
    /**
     *
     * @param modelPath
     */
    public void DoMultiClassClassification(int numIterations, String modelPath)
    {
        // Run training algorithm to build the model.
        this.model = SVMWithSGD.train(trainingData.rdd(), numIterations);
        // Clear the default threshold.
        model.clearThreshold(); 
        // Save and load model
        cleanModelDirectoryPathIfExists(new File(modelPath));
        this.model.save(this.sparkContext, modelPath); 
        System.out.println("saved model at = " + modelPath);
    }

    /**
     *
     * @param modelPath
     */
    public void PrintEvaluationMetrics(String modelPath)
    {
        // Compute raw scores on the test set.
        SVMModel evaluationModel = SVMModel.load(MultiClassificationSample.this.sparkContext, modelPath);
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testData.map((LabeledPoint p)
                -> 
                {

                    Double score = evaluationModel.predict(p.features());
                    return new Tuple2<>(score, p.label());
        });
        if (scoreAndLabels != null)
        {
            // Get evaluation metrics.
            BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
            double auROC = metrics.areaUnderROC();
            System.out.println("Area under ROC = " + auROC);
        } 
        else
        {
            System.out.println(">> No Scores and Labels to evaluate Metrics");
        }
    }

    /**
     *
     * @param path
     * @return
     */
    static public boolean cleanModelDirectoryPathIfExists(File path)
    {
        if (path.exists())
        {
            for (File file : path.listFiles())
            {
                if (file.isDirectory())
                {
                    cleanModelDirectoryPathIfExists(file);
                } 
                else
                {
                    file.delete();
                }
            }
        }
        return (path.delete());
    }
}
