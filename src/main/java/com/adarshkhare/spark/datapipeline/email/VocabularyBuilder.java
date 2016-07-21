/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.datapipeline.email;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author adkhare
 */
public class VocabularyBuilder
{
    public class VocabularyWord
    {
        public String word;
        public int Id;
    }
    
    private final Map<String, Integer> vocabMap;
    private int maxWordId = 0;
    
    private final String vocabFileName = "/Adarsh/eMailData/vocabulary.txt";

    /**
     *
     */
    public VocabularyBuilder()
    {
        vocabMap = this.loadExistingVocabulary();
    }
    
    /**
     * Add a new word in vocabulary if it is not already exist.
     * @param word
     * @return 0 if word already exist else return newly generated wordId.
     */
    public int addWordInVocabulary(String word)
    {
        if(!vocabMap.containsKey(word))
        {
            this.maxWordId++;
            this.vocabMap.put(word, maxWordId);
            return this.maxWordId;
        }
        return 0;
    }
    
    /**
     *
     */
    public void PrintLists()
    {
        System.out.println("Printing Vocab");
        this.vocabMap.values().stream().forEach(System.out::println);
    }
    
    /**
     *
     */
    public void SaveVocabulary()
    {
        try
        {
            try (FileOutputStream fos = new FileOutputStream(vocabFileName); ObjectOutputStream oos = new ObjectOutputStream(fos))
            {
                oos.writeObject(this.vocabMap);
            }
            Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.INFO, 
                    "Serialized HashMap data is saved. count = {0}", this.vocabMap.size());
        } 
        catch (IOException ioe)
        {
            Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.SEVERE, null, ioe);
        }
    }
    
    private Map<String, Integer> loadExistingVocabulary()
    {
        Map<String, Integer> tempWordMap = new HashMap<>();
        try
        {
            try (FileInputStream fis = new FileInputStream(vocabFileName); ObjectInputStream ois = new ObjectInputStream(fis))
            {
                tempWordMap = (HashMap) ois.readObject();
            }
            Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.INFO, 
                    "Vocabulary loaded with existing word count = {0}", tempWordMap.size());
        } catch (IOException | ClassNotFoundException ex)
        {
            Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tempWordMap;
    }
}
