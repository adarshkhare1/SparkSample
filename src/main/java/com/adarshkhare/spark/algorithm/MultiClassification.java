/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.algorithm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
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
public class MultiClassification
{
    private SVMModel model;
    private final SparkContext sparkContext;
    JavaRDD<LabeledPoint> testData;
    JavaRDD<LabeledPoint> trainingData;
    
    /**
     *
     * @param context
     */
    public MultiClassification(SparkContext context)
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
     * @param numIterations
     * @param modelDir
     * @return return the path where model is saved. Null if model is not successfully saved.
     */
    public String DoMultiClassClassification(int numIterations, String modelDir)
    {
        // Run training algorithm to build the model.
        this.model = SVMWithSGD.train(trainingData.rdd(), numIterations);
        // Clear the default threshold.
        model.clearThreshold();
        // Save and load model
        String savePath = createModelDirectory(modelDir);
        cleanModelDirectoryPathIfExists(new File(savePath));
        if(!StringUtils.isEmpty(savePath))
        {
            this.model.save(this.sparkContext, savePath);
            Logger.getLogger(MultiClassification.class.getName()).log(Level.INFO,
                                                                      "saved model at = {0}", savePath);    
        }
        else
        {
            Logger.getLogger(MultiClassification.class.getName()).log(Level.WARNING,
                                                                      "Could not save model.");
        }
        return savePath;
    }

    /**
     *
     * @param modelPath
     */
    public void PrintEvaluationMetrics(String modelPath)
    {
        // Compute raw scores on the test set.
        SVMModel evaluationModel = SVMModel.load(MultiClassification.this.sparkContext, modelPath);
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
            Logger.getLogger(MultiClassification.class.getName()).log(Level.INFO, 
                                                                            "Area under ROC = {0}", auROC);
        } 
        else
        {
            Logger.getLogger(MultiClassification.class.getName()).log(Level.WARNING,
                                                                            ">> No Scores and Labels to evaluate Metrics");
        }
    }

    private String createModelDirectory(String directoryName)
    {
        try
        {
            File tempFile = File.createTempFile("my_prefix", "");
            tempFile.delete();
            String absolutePath = tempFile.getAbsolutePath();
            String tempFilePath = absolutePath.
    		    substring(0,absolutePath.lastIndexOf(File.separator));
            Path modelDir = Paths.get(tempFilePath, directoryName);
            return modelDir.toString();
        } 
        catch (IOException ex)
        {
            Logger.getLogger(MultiClassification.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
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
