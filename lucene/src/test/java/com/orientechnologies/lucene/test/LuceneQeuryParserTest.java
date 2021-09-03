package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.InputStream;
import java.util.List;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 19/05/2016. */
public class LuceneQeuryParserTest extends BaseLuceneTest {

  @Before
  public void init() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
  }

  @Test
  public void shouldSearchWithLeadingWildcard() {

    // enabling leading wildcard
    db.command(
            new OCommandSQL(
                "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}"))
        .execute();

    // querying with leading wildcard
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [title] LUCENE \"(title:*tain)\""));

    assertThat(docs).hasSize(4);
  }

  @Test
  public void shouldSearchWithLowercaseExpandedTerms() {

    // enabling leading wildcard
    db.command(
            new OCommandSQL(
                "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE metadata {\"default\": \""
                    + KeywordAnalyzer.class.getCanonicalName()
                    + "\", \"lowercaseExpandedTerms\": false}"))
        .execute();

    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>("select * from Song where [author] LUCENE \"Hunter\""));

    assertThat(docs).hasSize(97);

    docs =
        db.query(
            new OSQLSynchQuery<ODocument>("select * from Song where [author] LUCENE \"HUNTER\""));

    assertThat(docs).hasSize(0);
  }
}
