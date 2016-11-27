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

/**
 *
 * @author adkhare
 */
public class EmailClassificationList
{
    private final List<String> classList;
    
    /**
     *
     */
    public EmailClassificationList()
    {
        classList = this.loadTerminollogyList("classes.txt");
    }
    
    /**
     *
     */
    public void PrintLists()
    {
        System.out.println("Printing Classes");
        classList.stream().forEach(System.out::println);
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
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return termList;
    }
    
     /**
     *
     * @param fileName
     * @return
     */
    private static File getFile(String fileName)
    {
        File file1 = new File(EMailExtractor.VOCAB_ROOT);
        File file2 = new File(file1, fileName);
        return file2;
    }
}
