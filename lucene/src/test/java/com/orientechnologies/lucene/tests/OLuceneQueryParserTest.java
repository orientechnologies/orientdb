package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 19/05/2016.
 */
public class OLuceneQueryParserTest extends OLuceneBaseTest {

  @Before
  public void init() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.execute("sql", getScriptFromStream(stream));

  }

  @Test
  public void shouldSearchWithLeadingWildcard() {

    //enabling leading wildcard
    db.command(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}");

    //querying with leading wildcard
    OResultSet docs = db.query("select * from Song where search_class(\"(title:*tain)\") = true");

    assertThat(docs).hasSize(4);
  }

  @Test
  public void shouldSearchWithLowercaseExpandedTerms() {

    //enabling leading wildcard
    db.command(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE metadata {\"default\": \"" + KeywordAnalyzer.class
            .getCanonicalName() + "\", \"lowercaseExpandedTerms\": false}");

    OResultSet docs = db.query("select * from Song where search_class('Hunter') =true");

    assertThat(docs).hasSize(97);

    docs = db.query("select * from Song where search_class('HUNTER')=true");

    assertThat(docs).hasSize(0);
  }

  @Test
  public void shouldFailIfLeadingWild() {

    //enabling leading wildcard
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}");

    //querying with leading wildcard
    OResultSet docs = db.query("select * from Song where search_class ('title:*tain')=true");

    assertThat(docs).hasSize(4);
  }

  @Test
  public void shouldUseBoostsFromQuery() throws Exception {
    //enabling leading wildcard
    db.command("create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE ");

    //querying with boost
    List<String> boostedDocs = db.query("select * from Song where search_class ('(title:forever)^2 OR author:Boudleaux')=true")
        .stream()
        .map(r -> r.<String>getProperty("title"))
        .collect(Collectors.toList());

    assertThat(boostedDocs).hasSize(5);

    //forever in title is boosted
    assertThat(boostedDocs).contains("THIS TIME FOREVER"
        , "FOREVER YOUNG"
        , "TOMORROW IS FOREVER"
        , "STARS AND STRIPES FOREVER" //boosted
        , "ALL I HAVE TO DO IS DREAM");

    List<String> docs = db.query("select * from Song where search_class ('(title:forever) OR author:Boudleaux')=true")
        .stream()
        .map(r -> r.<String>getProperty("title"))
        .collect(Collectors.toList());

    assertThat(docs).hasSize(5);

    //no boost, order changed
    assertThat(docs).contains("THIS TIME FOREVER"
        , "FOREVER YOUNG"
        , "TOMORROW IS FOREVER"
        , "ALL I HAVE TO DO IS DREAM"
        , "STARS AND STRIPES FOREVER"); //no boost, last position

  }

  @Test
  public void shouldUseBoostsFromMap() throws Exception {
    //enabling leading wildcard
    db.command("create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE ");

    //querying with boost
    List<String> boostedDocs = db
        .query("select * from Song where search_class ('title:forever OR author:Boudleaux' , {'boost':{ 'title': 2  }  })=true")
        .stream()
        .map(r -> r.<String>getProperty("title"))
        .collect(Collectors.toList());

    assertThat(boostedDocs).hasSize(5);

    //forever in title is boosted
    assertThat(boostedDocs).contains("THIS TIME FOREVER"
        , "FOREVER YOUNG"
        , "TOMORROW IS FOREVER"
        , "STARS AND STRIPES FOREVER" //boosted
        , "ALL I HAVE TO DO IS DREAM");

    List<String> docs = db.query("select * from Song where search_class ('(title:forever) OR author:Boudleaux')=true")
        .stream()
        .map(r -> r.<String>getProperty("title"))
        .collect(Collectors.toList());

    assertThat(docs).hasSize(5);

    //no boost, order changed
    assertThat(docs).contains("THIS TIME FOREVER"
        , "FOREVER YOUNG"
        , "TOMORROW IS FOREVER"
        , "ALL I HAVE TO DO IS DREAM"
        , "STARS AND STRIPES FOREVER"); //no boost, last position

  }

  @Test
  public void ahouldOverrideAnalyzer() throws Exception {

    //enabling leading wildcard
    db.command("create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE ");

    //querying with boost
    OResultSet resultSet = db.query("select * from Song where search_class ('title:forever OR author:boudleaux' , "
        + "{'customAnalysis': true, "
        + "  \"query\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\" } "
        + ")=true");

    assertThat(resultSet).hasSize(5);

  }
}

