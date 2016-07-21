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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author adkhare
 */
public class EMailExtractor
{
    private static final String DATA_ROOT = "C:/Adarsh/eMailData";
    
    private final Map<String, String> constString;
    
    
    /**
     *
     * @param parseNewMessages
     */
    public EMailExtractor(Boolean parseNewMessages)
    {
        constString = this.loadDictionary("constantStrings.txt");
        if(parseNewMessages)
        {
            SplitMessages();
        }
        
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
        } catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dict;
    }
    
    /**
     *
     * @param fileName
     * @return
     */
    public static File getFile(String fileName)
    {
        File file1 = new File(DATA_ROOT);
        File file2 = new File(file1, fileName);
        return file2;
    }

    private void SplitMessages()
    {
        File file = getFile("eMailSamples/eMails.txt");
        int msgCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            BufferedWriter bw = null;
            for (String line; (line = br.readLine()) != null;)
            {
                if (line.equals(constString.get("senderLine")))
                {
                    if (bw != null)
                    {
                        bw.close();
                    }
                    msgCount++;
                    File outFile = getFile("eMailSamples/" + msgCount + ".txt");
                    bw = new BufferedWriter(new FileWriter(outFile));
                }
                if (bw != null)
                {
                    bw.write(line+"\r\n");
                }
            }
            if (bw != null)
            {
                bw.close();
            }
            // line is not visible here.
        } catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

   
    
}
