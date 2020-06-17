package com.orientechnologies.lucene.analyzer;

import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.QUERY;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Created by frank on 30/10/2015. */
public class OLuceneAnalyzerFactoryTest {
  private OLuceneAnalyzerFactory analyzerFactory;
  private ODocument metadata;
  private OIndexDefinition indexDef;

  @Before
  public void before() throws IOException {
    analyzerFactory = new OLuceneAnalyzerFactory();
    // default analyzer is Standard
    // default analyzer for indexing is keyword
    // default analyzer for query is standard

    String metajson =
        OIOUtils.readFileAsString(new File("./src/test/resources/index_metadata_new.json"));
    metadata = new ODocument().fromJSON(metajson);
    indexDef = Mockito.mock(OIndexDefinition.class);
    when(indexDef.getFields())
        .thenReturn(asList("name", "title", "author", "lyrics", "genre", "description"));
    when(indexDef.getClassName()).thenReturn("Song");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createAnalyzerNullIndexDefinition() {
    analyzerFactory.createAnalyzer(null, INDEX, metadata);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createAnalyzerNullIndex() {
    analyzerFactory.createAnalyzer(indexDef, null, metadata);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createAnalyzerNullMetadata() {
    analyzerFactory.createAnalyzer(indexDef, INDEX, null);
  }

  @Test
  public void shouldAssignStandardAnalyzerForIndexingUndefined() throws Exception {
    OLucenePerFieldAnalyzerWrapper analyzer =
        (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef, INDEX, metadata);
    // default analyzer for indexing
    assertThat(analyzer.getWrappedAnalyzer("undefined")).isInstanceOf(StandardAnalyzer.class);
  }

  @Test
  public void shouldAssignKeywordAnalyzerForIndexing() throws Exception {
    OLucenePerFieldAnalyzerWrapper analyzer =
        (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef, INDEX, metadata);
    // default analyzer for indexing
    assertThat(analyzer.getWrappedAnalyzer("genre")).isInstanceOf(KeywordAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.genre")).isInstanceOf(KeywordAnalyzer.class);
  }

  @Test
  public void shouldAssignConfiguredAnalyzerForIndexing() throws Exception {
    OLucenePerFieldAnalyzerWrapper analyzer =
        (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef, INDEX, metadata);
    assertThat(analyzer.getWrappedAnalyzer("title")).isInstanceOf(EnglishAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.title")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("author")).isInstanceOf(KeywordAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.author")).isInstanceOf(KeywordAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("lyrics")).isInstanceOf(EnglishAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.lyrics")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("description")).isInstanceOf(StandardAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.description"))
        .isInstanceOf(StandardAnalyzer.class);

    StopwordAnalyzerBase description =
        (StopwordAnalyzerBase) analyzer.getWrappedAnalyzer("description");

    assertThat(description.getStopwordSet()).isNotEmpty();
    assertThat(description.getStopwordSet()).hasSize(2);
    assertThat(description.getStopwordSet().contains("the")).isTrue();
    assertThat(description.getStopwordSet().contains("is")).isTrue();
  }

  @Test
  public void shouldAssignConfiguredAnalyzerForQuery() throws Exception {
    OLucenePerFieldAnalyzerWrapper analyzer =
        (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef, QUERY, metadata);
    assertThat(analyzer.getWrappedAnalyzer("title")).isInstanceOf(EnglishAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.title")).isInstanceOf(EnglishAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("author")).isInstanceOf(KeywordAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.author")).isInstanceOf(KeywordAnalyzer.class);

    assertThat(analyzer.getWrappedAnalyzer("genre")).isInstanceOf(StandardAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.genre")).isInstanceOf(StandardAnalyzer.class);
  }

  @Test
  public void shouldUseClassNameToPrefixFieldName() {
    final OLucenePerFieldAnalyzerWrapper analyzer =
        (OLucenePerFieldAnalyzerWrapper) analyzerFactory.createAnalyzer(indexDef, QUERY, metadata);
    assertThat(analyzer.getWrappedAnalyzer("Song.title")).isInstanceOf(EnglishAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.author")).isInstanceOf(KeywordAnalyzer.class);
    assertThat(analyzer.getWrappedAnalyzer("Song.genre")).isInstanceOf(StandardAnalyzer.class);
  }
}
