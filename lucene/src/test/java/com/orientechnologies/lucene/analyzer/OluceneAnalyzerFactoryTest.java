package com.orientechnologies.lucene.analyzer;

import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.orientechnologies.lucene.analyzer.OluceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.orientechnologies.lucene.analyzer.OluceneAnalyzerFactory.AnalyzerKind.QUERY;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Created by frank on 30/10/2015.
 */
public class OluceneAnalyzerFactoryTest {

  private OluceneAnalyzerFactory analyzerFactory;
  private ODocument              metadata;
  private OIndexDefinition       indexDef;

  @BeforeTest
  public void before() {

    analyzerFactory = new OluceneAnalyzerFactory();

    //default analyzer is Standard
    //default analyzer for indexing is keyword
    //default analyzer for query is standard
    metadata = new ODocument()
        .fromJSON(
            "{"
            + "\"analyzer\":\"" + StandardAnalyzer.class.getName() + "\" , "
            + "\"index_analyzer\":\"" + KeywordAnalyzer.class.getName() + "\" , "
            + "\"title_index_analyzer\":\"" + EnglishAnalyzer.class.getName() + "\" , "
            + "\"title_query_analyzer\":\"" + EnglishAnalyzer.class.getName() + "\" , "
            + "\"author_query_analyzer\":\"" + KeywordAnalyzer.class.getName() + "\","
            + "\"lyrics_index_analyzer\":\"" + EnglishAnalyzer.class.getName() + "\""
            + "}");

    indexDef = Mockito.mock(OIndexDefinition.class);

    when(indexDef.getFields()).thenReturn(asList("title", "author", "lyrics", "genre"));

  }

  @Test
  public void shoulAssignStandardAnalyzerForIndexingUndefined() throws Exception {

    OLucenePerFieldAnalyzerWrapper analyzer = (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef,
                                                                                                              INDEX,
                                                                                                              metadata);
    //default analazer for indexing
    assertThat(analyzer.getWrappedAnalyzer("undefined")).isInstanceOf(StandardAnalyzer.class);

  }

  @Test
  public void shoulAssignKeywordAnalyzerForIndexing() throws Exception {

    OLucenePerFieldAnalyzerWrapper analyzer = (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef,
                                                                                                              INDEX,
                                                                                                              metadata);
    //default analazer for indexing
    assertThat(analyzer.getWrappedAnalyzer("genre")).isInstanceOf(KeywordAnalyzer.class);

  }

  @Test
  public void shoulAssignConfiguredAnalyzerForIndexing() throws Exception {

    OLucenePerFieldAnalyzerWrapper analyzer = (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef,
                                                                                                              INDEX,
                                                                                                              metadata);
    assertThat(analyzer.getWrappedAnalyzer("title")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("author")).isInstanceOf(KeywordAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("lyrics")).isInstanceOf(EnglishAnalyzer.class);

  }

  @Test
  public void shoulAssignConfiguredAnalyzerForQuery() throws Exception {

    OLucenePerFieldAnalyzerWrapper analyzer = (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef,
                                                                                                              QUERY,
                                                                                                              metadata);
    assertThat(analyzer.getWrappedAnalyzer("title")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("author")).isInstanceOf(KeywordAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("genre")).isInstanceOf(StandardAnalyzer.class);

  }

}