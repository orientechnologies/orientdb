/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by enricorisa on 19/09/14.
 */
public class LuceneMultiFieldTest extends BaseLuceneTest {

  public LuceneMultiFieldTest() {
    super();
  }

  @Before
  public void init() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL(
        "create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE METADATA {" + "\"title_index\":\""
            + EnglishAnalyzer.class.getName() + "\" , " + "\"title_query\":\"" + EnglishAnalyzer.class.getName() + "\" , "
            + "\"author_index\":\"" + StandardAnalyzer.class.getName() + "\"}")).execute();

    final ODocument index = db.getMetadata().getIndexManager().getIndex("Song.title_author").getMetadata();

    assertThat(index.<Object>field("author_index")).isEqualTo(StandardAnalyzer.class.getName());
    assertThat(index.<Object>field("title_index")).isEqualTo(EnglishAnalyzer.class.getName());

  }

  @Test
  public void testSelectSingleDocumentWithAndOperator() {

    List<ODocument> docs = db.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountain AND author:Fabbio)\""));
    //List<ODocument> docs = databaseDocumentTx.query(
    //        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountains)\""));

    assertThat(docs).hasSize(1);

  }

  @Test
  public void testSelectMultipleDocumentsWithOrOperator() {

    List<ODocument> docs = db.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"(title:mountain OR author:Fabbio)\""));

    assertThat(docs).hasSize(91);

  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnTitle() {

    List<ODocument> docs = db
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"mountain\""));

    assertThat(docs).hasSize(5);

  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnAuthor() {

    List<ODocument> docs = db
        .query(new OSQLSynchQuery<ODocument>("select * from Song where [title,author] LUCENE \"author:fabbio\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  @Ignore
  public void testSelectOnAuthorWithMatchOnAuthor() {
    //FIXME please
    List<ODocument> docs = db.query(
        new OSQLSynchQuery<ODocument>("select * from Song where [author,title] LUCENE \"(fabbio)\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  public void testSelectOnIndexWithIgnoreNullValuesToFalse() {
    //#5579
    String script = "create class Item\n"
        + "create property Item.title string\n"
        + "create property Item.summary string\n"
        + "create property Item.content string\n"
        + "create index Item.i_lucene on Item(title, summary, content) fulltext engine lucene METADATA {ignoreNullValues:false}\n"
        + "insert into Item set title = 'test', content = 'this is a test'\n";
    db.command(new OCommandScript("sql", script)).execute();

    List<ODocument> docs;

    docs = db.query(new OSQLSynchQuery<ODocument>("select * from Item where title lucene 'te*'"));

    assertThat(docs).hasSize(1);

    docs = db
        .query(new OSQLSynchQuery<ODocument>("select * from Item where [title, summary, content] lucene 'test'"));

    assertThat(docs).hasSize(1);

  }
}
