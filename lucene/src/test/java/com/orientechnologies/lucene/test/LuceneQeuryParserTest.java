package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 19/05/2016. */
public class LuceneQeuryParserTest extends BaseLuceneTest {

  @Before
  public void init() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.execute("sql", getScriptFromStream(stream)).close();
  }

  @Test
  public void shouldSearchWithLeadingWildcard() {

    // enabling leading wildcard
    db.command(
            "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata"
                + " {\"allowLeadingWildcard\": true}")
        .close();

    // querying with leading wildcard
    OResultSet docs = db.query("select * from Song where [title] LUCENE \"(title:*tain)\"");

    assertThat(docs.stream()).hasSize(4);
  }

  @Test
  public void shouldSearchWithLowercaseExpandedTerms() {

    // enabling leading wildcard
    db.command(
            "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE metadata"
                + " {\"default\": \""
                + KeywordAnalyzer.class.getCanonicalName()
                + "\", \"lowercaseExpandedTerms\": false}")
        .close();

    OResultSet docs = db.query("select * from Song where [author] LUCENE \"Hunter\"");

    assertThat(docs.stream()).hasSize(97);

    docs = db.query("select * from Song where [author] LUCENE \"HUNTER\"");

    assertThat(docs.stream()).hasSize(0);
  }
}
