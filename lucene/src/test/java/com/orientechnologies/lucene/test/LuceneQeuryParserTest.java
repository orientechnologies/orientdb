package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 19/05/2016.
 */
public class LuceneQeuryParserTest extends BaseLuceneTest {

  @Before
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);


  }

  @After
  public void deInit() {
    deInitDB();
  }

  @Test
  public void shouldSearchWithLeadingWildcard() {

    //enabling leading wildcard
    databaseDocumentTx.command(
        new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}"))
        .execute();

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
    //querying with leading wildcard
    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title] LUCENE \"(title:*tain)\""));

    assertThat(docs).hasSize(4);
  }

  @Test
  public void shouldFailIfLeadinWild() {

    //enabling leading wildcard
    databaseDocumentTx.command(
        new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {\"allowLeadingWildcard\": true}"))
        .execute();

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
    //querying with leading wildcard
    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title] LUCENE \"(title:*tain)\""));

    assertThat(docs).hasSize(4);
  }

}

