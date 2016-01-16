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

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by enricorisa on 19/09/14.
 */
public class LuceneMultiFieldTest extends BaseLuceneTest {

  public LuceneMultiFieldTest() {
    super();
  }

  @Before
  public void init() {
    initDB();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Song");
    song.setSuperClass(v);
    song.createProperty("title", OType.STRING);
    song.createProperty("author", OType.STRING);

    //    databaseDocumentTx.command(new OCommandSQL("create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE"))
    //        .execute();

    databaseDocumentTx.command(new OCommandSQL(
        "create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE METADATA {" + "\"title_index_analyzer\":\""
            + EnglishAnalyzer.class.getName() + "\" , " + "\"title_query_analyzer\":\"" + EnglishAnalyzer.class.getName() + "\" , "
            + "\"author_index_analyzer\":\"" + StandardAnalyzer.class.getName() + "\"}")).execute();

    final ODocument index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Song.title_author").getMetadata();

    assertThat(index.field("author_index_analyzer")).isEqualTo(StandardAnalyzer.class.getName());
    assertThat(index.field("title_index_analyzer")).isEqualTo(EnglishAnalyzer.class.getName());
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
  }

  @After
  public void deInit() {
    deInitDB();
  }

  @Test
  public void testSelectSingleDocumentWithAndOperator() {

    List<ODocument> docs = databaseDocumentTx.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountain AND author:Fabbio)\""));
    //List<ODocument> docs = databaseDocumentTx.query(
    //        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountains)\""));

    assertThat(docs).hasSize(1);

  }

  @Test
  public void testSelectMultipleDocumentsWithOrOperator() {

    List<ODocument> docs = databaseDocumentTx.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountain OR author:Fabbio)\""));

    assertThat(docs).hasSize(91);

  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnTitle() {

    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"mountain\""));

    assertThat(docs).hasSize(5);

  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnAuthor() {

    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"author:fabbio\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  @Ignore
  public void testSelectOnAuthorWithMatchOnAuthor() {
    //FIXME please
    List<ODocument> docs = databaseDocumentTx.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [author,title] LUCENE \"(fabbio)\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  public void testSelectOnIndexWithIgnoreNullValuesToFalse() {
    //#5579
    String script = "create class Item\n" + "create property Item.title string\n" + "create property Item.summary string\n"
        + "create property Item.content string\n"
        + "create index Item.i_lucene on Item(title, summary, content) fulltext engine lucene METADATA {ignoreNullValues:false}\n"
        + "insert into Item set title = 'test', content = 'this is a test'\n";
    databaseDocumentTx.command(new OCommandScript("sql", script)).execute();

    List<ODocument> docs;

    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select * from Item where title lucene 'te*'"));

    assertThat(docs).hasSize(1);

    docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select * from Item where [title, summary, content] lucene 'test'"));

    assertThat(docs).hasSize(1);

  }
}
