/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.datapipeline.email;

import static com.adarshkhare.spark.datapipeline.email.EMailExtractor.getFile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
}
