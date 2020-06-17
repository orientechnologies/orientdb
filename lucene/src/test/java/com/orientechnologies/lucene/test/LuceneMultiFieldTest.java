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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by enricorisa on 19/09/14. */
public class LuceneMultiFieldTest extends BaseLuceneTest {

  public LuceneMultiFieldTest() {
    super();
  }

  @Before
  public void init() throws Exception {
    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      //noinspection deprecation
      db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();
    }

    //noinspection deprecation
    db.command(
            new OCommandSQL(
                "create index Song.title_author on Song (title,author) FULLTEXT ENGINE LUCENE METADATA {"
                    + "\"title_index\":\""
                    + EnglishAnalyzer.class.getName()
                    + "\" , "
                    + "\"title_query\":\""
                    + EnglishAnalyzer.class.getName()
                    + "\" , "
                    + "\"author_index\":\""
                    + StandardAnalyzer.class.getName()
                    + "\"}"))
        .execute();

    final ODocument index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Song.title_author").getMetadata();

    assertThat(index.<Object>field("author_index")).isEqualTo(StandardAnalyzer.class.getName());
    assertThat(index.<Object>field("title_index")).isEqualTo(EnglishAnalyzer.class.getName());
  }

  @Test
  public void testSelectSingleDocumentWithAndOperator() {

    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [title,author] LUCENE \"(title:mountain AND author:Fabbio)\""));
    assertThat(docs).hasSize(1);
  }

  @Test
  public void testSelectSingleDocumentWithAndOperatorNEwExec() {
    try (OResultSet docs =
        db.query(
            "select * from Song where [title,author] LUCENE \"(title:mountain AND author:Fabbio)\"")) {

      assertThat(docs.hasNext()).isTrue();
      docs.next();
      assertThat(docs.hasNext()).isFalse();
    }
  }

  @Test
  public void testSelectMultipleDocumentsWithOrOperator() {
    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [title,author] LUCENE \"(title:mountain OR author:Fabbio)\""));

    assertThat(docs).hasSize(91);
  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnTitle() {
    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [title,author] LUCENE \"mountain\""));

    assertThat(docs).hasSize(5);
  }

  @Test
  public void testSelectOnTitleAndAuthorWithMatchOnAuthor() {
    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [title,author] LUCENE \"author:fabbio\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  @Ignore
  public void testSelectOnAuthorWithMatchOnAuthor() {
    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where [author,title] LUCENE \"(fabbio)\""));

    assertThat(docs).hasSize(87);
  }

  @Test
  public void testSelectOnIndexWithIgnoreNullValuesToFalse() {
    // #5579
    String script =
        "create class Item\n"
            + "create property Item.Title string\n"
            + "create property Item.Summary string\n"
            + "create property Item.Content string\n"
            + "create index Item.i_lucene on Item(Title, Summary, Content) fulltext engine lucene METADATA {ignoreNullValues:false}\n"
            + "insert into Item set Title = 'wrong', content = 'not me please'\n"
            + "insert into Item set Title = 'test', content = 'this is a test'\n";
    //noinspection deprecation
    db.command(new OCommandScript("sql", script)).execute();

    @SuppressWarnings("deprecation")
    List<ODocument> docs =
        db.query(new OSQLSynchQuery<ODocument>("select * from Item where Title lucene 'te*'"));
    assertThat(docs).hasSize(1);

    //noinspection deprecation
    docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Item where [Title, Summary, Content] lucene 'test'"));
    assertThat(docs).hasSize(1);

    // nidex api
    final OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Item.i_lucene");
    try (Stream<ORID> stream = index.getInternal().getRids("(Title:test )")) {
      assertThat(stream.findAny().isPresent()).isTrue();
    }
  }
}
