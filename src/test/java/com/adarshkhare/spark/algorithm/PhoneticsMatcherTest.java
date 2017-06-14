package com.adarshkhare.spark.algorithm;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by adkhare on 6/14/2017.
 */
public class PhoneticsMatcherTest {
    @Test
    public void getPhoneticsCode() throws Exception
    {
        String code1 = PhoneticsMatcher.GetPhoneticsCode("beer");
        String code2 = PhoneticsMatcher.GetPhoneticsCode("bear");
        Assert.assertTrue("PhoneticsMatch", code1.equals(code2));
    }

    @Test
    public void areWordsPhoneticsMatch() throws Exception
    {
        Assert.assertTrue("PhoneticsMatch", PhoneticsMatcher.AreWordsPhoneticsMatch("bear", "beer"));
        Assert.assertTrue("PhoneticsMatch", PhoneticsMatcher.AreWordsPhoneticsMatch("Bear", "beer"));
    }

}