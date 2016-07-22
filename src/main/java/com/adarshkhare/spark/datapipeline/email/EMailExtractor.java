/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.datapipeline.email;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class EMailExtractor
{

    public static final String DATA_ROOT = "C:/Adarsh/eMailData";

    private final Map<String, String> constString;
    private final VocabularyBuilder vb;

    /**
     *
     */
    public EMailExtractor()
    {
        constString = this.loadDictionary("constantStrings.txt");
        vb = new VocabularyBuilder();
    }
    
    /**
     *
     * @param messageFilePath
     */
    public void ParseMessages(String messageFilePath)
    {
        this.SplitMessages(messageFilePath);
    }

    private Map<String, String> loadDictionary(String fileName)
    {
        Map<String, String> dict = new HashMap<>();
        File file = getFile(fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            for (String line; (line = br.readLine()) != null;)
            {
                String[] split = line.split("=");
                dict.put(split[0], split[1]);
            }
            // line is not visible here.
        } 
        catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dict;
    }

   
    
    private void SplitMessages(String messageFilePath)
    {
        File file = getFile(messageFilePath);
        int msgCount = 0;
       
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            BufferedWriter bw = null;
            File msgFile = null;
            List<Tuple2<Integer, Integer>> msgMap = new ArrayList<>();
            for (String line; (line = br.readLine()) != null;)
            {
                if (line.equals(constString.get("senderLine")))
                {
                    if (bw != null)
                    {
                        bw.close();
                    }
                    msgCount++;
                    msgFile = getMsgOutputFile(msgCount);
                    bw = new BufferedWriter(new FileWriter(msgFile));
                }
                if (bw != null)
                {
                    bw.write(line + "\r\n");
                }
            }
            if (bw != null)
            {
                bw.close();
                if (msgFile != null)
                {
                    msgMap = vb.getDataMapForMessage(msgFile.getAbsolutePath());
                    StringBuilder messageData = new StringBuilder();
                    msgMap.stream().forEach((result)-> {
                        messageData.append(result._1).append(":").append(result._2).append(" ");
                    });
                    Logger.getLogger(EMailExtractor.class.getName()).log(Level.INFO, messageData.toString());
                    
                }
            }
            // line is not visible here.
        } catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static File getMsgOutputFile(int msgCount)
    {
        String msgFilePath = "eMailSamples/" + msgCount + "/" + msgCount + ".txt";
        File file1 = new File(EMailExtractor.DATA_ROOT);
        File msgFile = new File(file1, msgFilePath);
        return msgFile;
    }
    
     /**
     *
     * @param fileName
     * @return
     */
    private static File getFile(String fileName)
    {
        File file1 = new File(EMailExtractor.DATA_ROOT);
        File file2 = new File(file1, fileName);
        return file2;
    }
}
