package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchFunctionTest extends BaseLuceneTest {

  @Test
  public void shouldSearchOnIndex() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();
    db.command(new OCommandSQL(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();

    List<ODocument> results = db.command(new OCommandSQL("SELECT from song where SEARCH_INDEX(Song.title, 'BELIEVE') = true"))
        .execute();

    assertThat(results).hasSize(2);

    OResultSet resultSet = db.query("SELECT from Song where SEARCH_INDEX(Song.title, 'BELIEVE') = true");

    System.out.println();
    resultSet.getExecutionPlan().ifPresent(x-> System.out.println(x.prettyPrint(0, 2)));
    assertThat(resultSet).hasSize(2);

//    //no params: match tentative
//    db.query("SELECT from song where LUCENE_SEARCH(['title'], ?, {} ) = true");
//
//    //field name:
//    db.query("SELECT from song where LUCENE_SEARCH(?, title ) = true");
//
//    //field name plus metadata:
//    db.query("SELECT from song where LUCENE_SEARCH('pippo', title , metadata {}) = true");
//
//    //only metadata
//    db.query("SELECT from song where LUCENE_SEARCH('title:pippo', {}) = true");
//
//    db.query("SELECT from song where LUCENE_SEARCH('title:pippo') = true");

    /**
     * fields : []
     * indeName: string
     * allowLeadingwilcard
     * bootsfiled
     *
     *
     */
  }
}
