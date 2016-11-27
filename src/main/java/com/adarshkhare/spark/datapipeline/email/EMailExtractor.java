/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.datapipeline.email;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class EMailExtractor
{
    public static final String MASTER_DATA_ROOT = "/Users/adkhare";
    public static final String DATA_ROOT = MASTER_DATA_ROOT+"/Adarsh/eMailData";
    public static final String MESSAGE_DIR_ROOT = MASTER_DATA_ROOT+"/Adarsh/eMailData/eMailSamples/messages/";
    public static final String VOCAB_ROOT = "sample/vocabulary";
    

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
        vb.SaveVocabulary(); // save the updated vocabulary.
    }

    private Map<String, String> loadDictionary(String fileName)
    {
        Map<String, String> dict = new HashMap<>();
        File file = getDataFile(fileName);
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
        File file = getDataFile(messageFilePath);
        int msgCount = 0;
        int recordedWordCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            BufferedWriter bw = null;
            File msgFile;
            List<Tuple2<Integer, Integer>> msgMap = new ArrayList<>();
            for (String line; (line = br.readLine()) != null;)
            {
                if (line.equals(constString.get("senderLine")))
                {
                    if (bw != null)
                    {
                        bw.close();
                        int numRecordEntry = SaveMessageAsData(msgCount);
                        recordedWordCount += numRecordEntry;
                    }
                    msgCount++;
                    msgFile = getMsgOutputFile(msgCount);
                    if (msgFile != null)
                    {
                        bw = new BufferedWriter(new FileWriter(msgFile));
                    }
                }
                if (bw != null)
                {
                    bw.write(line + "\r\n");
                }
            }
            if (bw != null)
            {
                bw.close();
                SaveMessageAsData(msgCount);
                int numRecordEntry = SaveMessageAsData(msgCount);
                recordedWordCount += numRecordEntry;
            }
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.INFO, 
                    "Average number of word entry per message = {0}", ((recordedWordCount*1.0)/msgCount));
        } catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private int SaveMessageAsData(int msgCount)
    {
        File msgFile = getMsgOutputFile(msgCount);
        int numRecordEntries = 0;
        if (msgFile.exists())
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.FINEST, "{0}..", msgCount);
            BufferedWriter bw;
            try
            {
                List<Tuple2<Integer, Integer>> msgMap;
                msgMap = vb.getDataMapForMessage(msgFile.getAbsolutePath());
                StringBuilder messageData = new StringBuilder();
                for (int i = 0; i < msgMap.size(); i++)
                {
                    Tuple2<Integer, Integer> result = msgMap.get(i);
                    if (result._1 != VocabularyBuilder.IGNORE_WORD_ID)
                    {
                        numRecordEntries++;
                        messageData.append(result._1).append(":").append(result._2).append(" ");
                    }
                }
                File msgDir = new File(MESSAGE_DIR_ROOT, msgCount + "/");
                File recordFile = new File(msgDir,  "dataRecord.txt");
                bw = new BufferedWriter(new FileWriter(recordFile));
                bw.write(messageData.toString());
                bw.close();
            } 
            catch (IOException ex)
            {
                Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return numRecordEntries;
    }

    private static File getMsgOutputFile(int msgCount)
    {        
        File msgDir = new File(MESSAGE_DIR_ROOT, msgCount+"/");
        if (!msgDir.exists())
        {
            msgDir.mkdir();
        }
        if (msgDir.exists())
        {
            File msgFile = new File(msgDir, msgCount + ".txt");
            return msgFile;
        } 
        else
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.WARNING, 
                    "Failed to create directory {0} to save message ", msgDir.getAbsolutePath());
            return null;
        }
    }
    
     /**
     *
     * @param fileName
     * @return
     */
    private static File getDataFile(String fileName)
    {
        File file1 = new File(EMailExtractor.DATA_ROOT);
        File file2 = new File(file1, fileName);
        return file2;
    }
}
