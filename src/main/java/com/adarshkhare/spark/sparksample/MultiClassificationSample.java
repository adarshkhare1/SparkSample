/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import java.util.List;
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
    
    /**
     *
     * @param context
     */
    public MultiClassificationSample(SparkContext context)
    {
        sparkContext = context;
    }
    
    /**
     *
     * @param dataPath
     * @return
     */
    public JavaRDD<Tuple2<Object, Object>> DoMultiClassClassification(String dataPath)
    {
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(this.sparkContext, dataPath).toJavaRDD();

        // Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
        training.cache();
        JavaRDD<LabeledPoint> test = data.subtract(training);

        // Run training algorithm to build the model.
        int numIterations = 100;
        this.model = SVMWithSGD.train(training.rdd(), numIterations);

        // Clear the default threshold.
        model.clearThreshold();

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map((LabeledPoint p) ->
        {
            Double score = model.predict(p.features());
            return new Tuple2<>(score, p.label());
        });
        
        return scoreAndLabels;
    }

    /**
     *
     * @param path
     * @return
     */
    public SVMModel SaveAndLoadModel(String path)
    { 
        // Save and load model
        this.model.save(this.sparkContext, path);
        return SVMModel.load(this.sparkContext, path);
    }

    /**
     *
     * @param scoreAndLabels
     */
    public void PrintEvaluationMetrics(JavaRDD<Tuple2<Object, Object>> scoreAndLabels)
    {
        // Get evaluation metrics.
        BinaryClassificationMetrics metrics
                = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

       
    }
}
