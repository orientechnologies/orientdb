/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;

import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;

/**
 * Created by enricorisa on 26/09/14.
 */

public class LuceneCreateIndexTest extends LuceneSingleFieldEmbeddedTest {

  public LuceneCreateIndexTest() {

    super();
    OLogManager.instance().installCustomFormatter();
    OLogManager.instance().setConsoleLevel(Level.INFO.getName());

  }

  @Override
  public void loadAndTest() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    databaseDocumentTx.command(new OCommandSQL(
        "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();
    databaseDocumentTx.command(new OCommandSQL(
        "create index Song.author on Song (author) FULLTEXT ENGINE LUCENE METADATA {\"analyzer\":\"" + StandardAnalyzer.class
            .getName() + "\"}")).execute();

    ODocument doc = new ODocument("Song");

    doc.field("title", "Local");
    doc.field("author", "Local");

    databaseDocumentTx.save(doc);
    testMetadata();
    assertQuery();

    assertNewQuery();

    databaseDocumentTx.close();

    databaseDocumentTx.open("admin", "admin");

    assertQuery();

    assertNewQuery();
  }

  protected void testMetadata() {
    final ODocument index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Song.title").getMetadata();

    Assert.assertEquals(index.field("analyzer"), StandardAnalyzer.class.getName());
  }

  protected void assertQuery() {
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
        "select * from Song where [title] LUCENE \"(title:mountain)\""));

    Assert.assertEquals(docs.size(), 4);

    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select * from Song where [author] LUCENE \"(author:Fabbio)\""));

    Assert.assertEquals(docs.size(), 87);

    String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and [author] LUCENE \"(author:Fabbio)\"";
    //String query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and author = 'Fabbio'";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(docs.size(), 1);

    query = "select * from Song where [title] LUCENE \"(title:mountain)\"  and author = 'Fabbio'";
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(docs.size(), 1);
  }

  protected void assertNewQuery() {

    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
        "select * from Song where [title] LUCENE \"(title:Local)\""));

    Assert.assertEquals(docs.size(), 1);
  }

  @Before
  @Override
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);
  }

}
