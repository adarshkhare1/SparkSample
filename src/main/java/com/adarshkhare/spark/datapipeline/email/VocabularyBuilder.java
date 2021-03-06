/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.spark.datapipeline.email;

import com.adarshkhare.spark.algorithm.MapReduce;
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.adarshkhare.spark.algorithm.PhoneticsMatcher;
import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

/**
 *
 * @author adkhare
 */
public class VocabularyBuilder
{
    private final Map<String, Integer> vocabMap;
    private final Set<String> ignoreWordSet;
    private int maxWordId = 0;
    public static final Set<String> KNOWN_MESSAGES
            = new HashSet<>(Arrays.asList("printing shipping labels on ebay", "fedex smart post claim details"));

    private final String vocabFileName = EMailExtractor.MASTER_DATA_ROOT + "/Adarsh/eMailData/vocabulary.txt";

    /**
     * WordId = -1 for ignorable words.
     */
    public static final int IGNORE_WORD_ID = -1;

    /**
     *
     */
    public VocabularyBuilder()
    {
        vocabMap = this.loadExistingVocabulary();
        ignoreWordSet = this.LoadIgnoreWordList();
    }

    public List<Tuple2<Integer, Integer>> getDataMapForMessage(String msgFile)
    {
        List<Tuple2<String, Integer>> counts = MapReduce.DoWordCount(msgFile);
        List<Tuple2<Integer, Integer>> wordMap = new ArrayList<>();
        counts.stream().forEach((result)
                -> 
                {
                    wordMap.add(new Tuple2<>(this.getWordId(result._1), result._2));
        });
        return wordMap;

    }

    /**
     * Add a new word in vocabulary if it is not already exist.
     *
     * @param word
     * @return 0 if word already exist else return newly generated wordId.
     */
    private int getWordId(String word)
    {
        if (StringUtils.isNotEmpty(word))
        {
            String normalizedWord = this.normalizeWord(word);
            if (this.IsValidWordForAnalysis(normalizedWord))
            {
                if (!vocabMap.containsKey(normalizedWord))
                {
                    this.vocabMap.put(normalizedWord, ++maxWordId);
                    return this.maxWordId;
                }
                else
                {
                    return vocabMap.get(normalizedWord);
                }
            }
        }
        return VocabularyBuilder.IGNORE_WORD_ID;
    }

    private String normalizeWord(String word)
    {
        return PhoneticsMatcher.GetPhoneticsCode(word.toLowerCase().trim());
    }

    private boolean IsValidWordForAnalysis(String normalizedWord)
    {
        return (KNOWN_MESSAGES.contains(normalizedWord))
                || 
                (StringUtils.isNotEmpty(normalizedWord)
                    && StringUtils.isAlpha(normalizedWord)
                    && !this.ignoreWordSet.contains(normalizedWord));
    }

    /**
     *
     */
    public void PrintVocabulary()
    {
        System.out.println("Printing Vocab");
        this.vocabMap.keySet().stream().sorted().forEach((String k)
                -> 
                {
                    System.out.println(k + "," + this.vocabMap.get(k));
        });
    }

    /**
     *
     */
    public void SaveVocabulary()
    {
        try
        {
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(vocabFileName)))
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

    private Set<String> LoadIgnoreWordList()
    {
        Set<String> termList = new HashSet<>();
        File file1 = new File(EMailExtractor.VOCAB_ROOT);
        File file = new File(file1, "IgnoreWordList.txt");
        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            for (String line; (line = br.readLine()) != null;)
            {
                termList.add(line);
            }
            // line is not visible here.
        }
        catch (IOException ex)
        {
            Logger.getLogger(EMailExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return termList;
    }

    private Map<String, Integer> loadExistingVocabulary()
    {
        Map<String, Integer> tempWordMap = new HashMap<>();
        try
        {
            File vocabFile = new File(vocabFileName);
            if (vocabFile.exists())
            {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(vocabFileName)))
                {
                    tempWordMap = (HashMap) ois.readObject();
                }
                Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.INFO,
                        "Vocabulary loaded with existing word count = {0}", tempWordMap.size());
            }
        }
        catch (IOException | ClassNotFoundException ex)
        {
            Logger.getLogger(VocabularyBuilder.class.getName()).log(Level.INFO, null, ex);
        }
        if (tempWordMap.isEmpty())
        {
            //Add default strings in vocabulary if vocabulary is empty
            for (String x : KNOWN_MESSAGES)
            {
                tempWordMap.put(x, ++maxWordId);
            }
        }
        return tempWordMap;
    }
}
