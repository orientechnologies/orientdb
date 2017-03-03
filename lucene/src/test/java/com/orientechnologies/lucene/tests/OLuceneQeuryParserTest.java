package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 19/05/2016.
 */
public class OLuceneQeuryParserTest extends OLuceneBaseTest {

  @Before
  public void init() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.execute("sql", getScriptFromStream(stream));

  }

  @Test
  public void shouldSearchWithLeadingWildcard() {

    //enabling leading wildcard
    db.command(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}")
        ;

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
  public void shouldFailIfLeadinWild() {

    //enabling leading wildcard
    db.command(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}")
        ;

    //querying with leading wildcard
    OResultSet docs = db.query("select * from Song where search_class ('title:*tain')=true");

    assertThat(docs).hasSize(4);
  }

}

