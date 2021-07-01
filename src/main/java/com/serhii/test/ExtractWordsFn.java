package com.serhii.test;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;

public class ExtractWordsFn extends DoFn<String, String> {

    public static final String TOKENIZER_PATTERN1 = "[^\\p{L}]+";
    public static final String TOKENIZER_PATTERN2 = "[^\\p{S}]+";


    @ProcessElement
    public List<String> processElementVal(ProcessContext c) {
        List<String> listVal= new ArrayList<>();
        for(String word: c.element().split(TOKENIZER_PATTERN1)) {
            if (!word.isEmpty()) {
                listVal.add(word);
                System.out.println(word);
                c.output(word);
            }
        }
        return listVal;
    }

    @ProcessElement
    public List<String> processElementKey(ProcessContext c) {
        List<String> listKey = new ArrayList<>();
        for (String word : c.element().split(TOKENIZER_PATTERN2)) {
            if (!word.isEmpty()) {
                listKey.add(word);
                System.out.println(word);
                c.output(word);
            }
        }
        return listKey;
    }


}
