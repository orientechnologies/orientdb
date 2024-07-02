package com.orientechnologies.lucene.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by frank on 20/09/2016. */
public class OLuceneAllIndexTest extends BaseLuceneTest {

  @Before
  public void init() throws IOException {

    System.setProperty("orientdb.test.env", "ci");

    String fromStream =
        OIOUtils.readStreamAsString(ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql"));
    db.execute("sql", fromStream).close();
    db.setProperty("CUSTOM", "strictSql=false");

    // three separate indeexs, one result
    db.command(
            "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA"
                + " {\"index_analyzer\":\""
                + StandardAnalyzer.class.getName()
                + "\"}")
        .close();

    db.command(
            "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA"
                + " {\"index_analyzer\":\""
                + StandardAnalyzer.class.getName()
                + "\"}")
        .close();

    db.command(
            "create index Song.lyrics on Song (lyrics) FULLTEXT ENGINE LUCENE METADATA"
                + " {\"index_analyzer\":\""
                + EnglishAnalyzer.class.getName()
                + "\"}")
        .close();
  }

  @Test
  @Ignore // FIXME: No function with name 'lucene_match'
  public void testLuceneFunction() {
    OResultSet docs =
        db.query("select from Song where lucene_match( \"Song.author:Fabbio\" ) = true ");
    assertThat(docs).hasSize(87);
  }
}
