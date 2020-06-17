package com.orientechnologies.lucene.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by frank on 20/09/2016. */
public class OLuceneAllIndexTest extends BaseLuceneTest {

  @Before
  public void init() throws IOException {
    OLogManager.instance().installCustomFormatter();
    OLogManager.instance().setConsoleLevel(Level.INFO.getName());

    System.setProperty("orientdb.test.env", "ci");

    String fromStream =
        OIOUtils.readStreamAsString(ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql"));
    db.command(new OCommandScript("sql", fromStream)).execute();
    db.setProperty("CUSTOM", "strictSql=false");

    //    db.command(new OCommandSQL(
    //        "create index Song.all on Song (title,author,lyrics) FULLTEXT ENGINE LUCENE METADATA
    // {"
    //            + "\"title_index_analyzer\":\"" + StandardAnalyzer.class.getName() + "\" , " +
    // "\"author_index_analyzer\":\""
    //            + StandardAnalyzer.class.getName() + "\" , " + "\"lyrics_index_analyzer\":\"" +
    // EnglishAnalyzer.class.getName()
    //            + "\"}")).execute();

    // three separate indeexs, one result
    db.command(
            new OCommandSQL(
                "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"index_analyzer\":\""
                    + StandardAnalyzer.class.getName()
                    + "\"}"))
        .execute();

    db.command(
            new OCommandSQL(
                "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"index_analyzer\":\""
                    + StandardAnalyzer.class.getName()
                    + "\"}"))
        .execute();

    db.command(
            new OCommandSQL(
                "create index Song.lyrics on Song (lyrics) FULLTEXT ENGINE LUCENE METADATA {\"index_analyzer\":\""
                    + EnglishAnalyzer.class.getName()
                    + "\"}"))
        .execute();
  }

  @Test
  @Ignore // FIXME: No function with name 'lucene_match'
  public void testLuceneFunction() {
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select from Song where lucene_match( \"Song.author:Fabbio\" ) = true "));
    assertThat(docs).hasSize(87);
  }
}
