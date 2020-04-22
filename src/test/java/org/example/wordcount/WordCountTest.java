package org.example.wordcount;

import org.example.wordcount.WordCount;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.Map;

public class WordCountTest {
    @Test
    public void wordCountTest() {
        Map<String, Integer> countedWords = WordCount.countWords();
        MatcherAssert.assertThat(countedWords.get("foo"), CoreMatchers.is(3));
        MatcherAssert.assertThat(countedWords.get("bar"), CoreMatchers.is(5));
    }

}