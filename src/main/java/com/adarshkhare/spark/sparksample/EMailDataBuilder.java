/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.sparksample;

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

/**
 *
 * @author adkhare
 */
public class EMailDataBuilder
{
    private static final String DATA_ROOT = "C:/Adarsh/eMailData";
    
    private final List<String> vocabList;
    private final List<String> classList;
    private final Map<String, String> constString;
    
    
    /**
     *
     */
    public EMailDataBuilder()
    {
        vocabList = this.loadTerminollogyList("vocabulary.txt");
        classList = this.loadTerminollogyList("classes.txt");
        constString = this.loadDictionary("constantStrings.txt");
        SplitMessages();
    }
    
    /**
     *
     */
    public void PrintLists()
    {
        System.out.println("Printing Vocab");
        vocabList.stream().forEach((value) ->
        {
            System.out.println(value);
        });
        System.out.println("Printing Classes");
        classList.stream().forEach((value) ->
        {
            System.out.println(value);
        });
    }
    
    private List<String> loadTerminollogyList(String fileName)
    {
        List<String> termList = new ArrayList<>();
        File file = getFile(fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            for (String line; (line = br.readLine()) != null;)
            {
                String[] split = line.split(":");
                termList.add(split[1]);
            }
            // line is not visible here.
        } catch (IOException ex)
        {
            Logger.getLogger(EMailDataBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
        return termList;
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
            Logger.getLogger(EMailDataBuilder.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(EMailDataBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

   
    
}
