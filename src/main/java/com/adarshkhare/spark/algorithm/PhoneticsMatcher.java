package com.adarshkhare.spark.algorithm;

import com.google.common.base.Strings;
import org.apache.commons.codec.language.Soundex;

public class PhoneticsMatcher
{
    public static String GetPhoneticsCode(String word)
    {
        Soundex soundex = new Soundex();
        return soundex.encode(word);
    }

    public static boolean AreWordsPhoneticsMatch(String word1, String word2)
    {
        if(Strings.isNullOrEmpty(word1) || Strings.isNullOrEmpty(word1))
            return false;
        Soundex soundex = new Soundex();
        return soundex.encode(word1).equals(soundex.encode(word2));
    }
}
