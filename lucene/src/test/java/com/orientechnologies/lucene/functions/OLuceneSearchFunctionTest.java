package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchFunctionTest extends BaseLuceneTest{

  @Test
  public void name() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();
    db.command(new OCommandSQL(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();

    List<ODocument> results = db.command(new OCommandSQL("SELECT from song where LUCENE_SEARCH(Song.title, 'BELIEVE') = true")).execute();

    System.out.println("results = " + results);
  }
}
