/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

import com.adarshkhare.spark.datapipeline.email.EMailExtractor;
import com.adarshkhare.spark.datapipeline.email.VocabularyBuilder;
import java.io.*;
import org.apache.commons.lang.StringUtils;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author adkhare
 */
public class SparkHelloWorld
{

    public static void main(String[] args) throws Exception
    {
        try
        {
            String selection = SparkHelloWorld.waitForEnterKey("Select Sample 1.parseMessages 2.buildVoab");
            switch (selection)
            {
                case "1":
                    SparkHelloWorld.convertMessageFileToDataRecords();
                    break;
                case "2":
                   SparkHelloWorld.PrintVocabulary();
                   break;
                default:
                    SparkHelloWorld.PrintVocabulary();
                    break;
            }

        } 
        finally
        {
            SparkHelloWorld.waitForEnterKey("Press <Enter> to teminate the program.");
        }
    }
    
    private static void convertMessageFileToDataRecords()
    {
        String messageFilePath = "eMailSamples/eMails.txt";
        EMailExtractor emExtractor = new EMailExtractor();
        emExtractor.ParseMessages(messageFilePath);
        
    }
    
    private static void PrintVocabulary()
    {
        VocabularyBuilder vb = new VocabularyBuilder();
        vb.PrintVocabulary();
        //vb.SaveVocabulary();
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
            Logger.getLogger("SparkSample").log(Level.SEVERE, null, ex);
        }
        return StringUtils.EMPTY;
    }
}
