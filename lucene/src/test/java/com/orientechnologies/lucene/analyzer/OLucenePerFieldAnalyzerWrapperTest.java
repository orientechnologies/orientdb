package com.orientechnologies.lucene.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

/** Created by frank on 30/11/2015. */
public class OLucenePerFieldAnalyzerWrapperTest {

  @Test
  public void shouldReturnDefaultAnalyzerForEachField() {
    final OLucenePerFieldAnalyzerWrapper analyzer =
        new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());

    assertThat(analyzer.getWrappedAnalyzer("a_field")).isNotNull();
    assertThat(analyzer.getWrappedAnalyzer("a_field")).isInstanceOf(StandardAnalyzer.class);
  }

  @Test
  public void shouldReturnCustomAnalyzerForEachField() {
    final OLucenePerFieldAnalyzerWrapper analyzer =
        new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());

    analyzer.add("text_en", new EnglishAnalyzer());
    analyzer.add("text_it", new ItalianAnalyzer());

    assertThat(analyzer.getWrappedAnalyzer("text_en")).isNotNull();
    assertThat(analyzer.getWrappedAnalyzer("text_en")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("text_it")).isNotNull();
    assertThat(analyzer.getWrappedAnalyzer("text_it")).isInstanceOf(ItalianAnalyzer.class);
  }

  @Test
  public void shouldReturnCustomAnalyzerForEachFieldInitializedByConstructor() {
    final OLucenePerFieldAnalyzerWrapper analyzer =
        new OLucenePerFieldAnalyzerWrapper(
            new StandardAnalyzer(),
            new HashMap<String, Analyzer>() {
              {
                put("text_en", new EnglishAnalyzer());
                put("text_it", new ItalianAnalyzer());
              }
            });
    assertThat(analyzer.getWrappedAnalyzer("text_en")).isNotNull();
    assertThat(analyzer.getWrappedAnalyzer("text_en")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("text_it")).isNotNull();
    assertThat(analyzer.getWrappedAnalyzer("text_it")).isInstanceOf(ItalianAnalyzer.class);
  }
}
