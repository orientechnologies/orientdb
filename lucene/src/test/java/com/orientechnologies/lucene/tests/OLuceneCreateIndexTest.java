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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 26/09/14. */
public class OLuceneCreateIndexTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(
            new OCommandSQL(
                "create index Song.title on Song (title) fulltext ENGINE LUCENE METADATA {\"analyzer\":\""
                    + StandardAnalyzer.class.getName()
                    + "\"}"))
        .execute();
    db.command(
            new OCommandSQL(
                "create index Song.author on Song (author) FULLTEXT ENGINE lucene METADATA {\"analyzer\":\""
                    + StandardAnalyzer.class.getName()
                    + "\"}"))
        .execute();

    OVertex doc = db.newVertex("Song");

    doc.setProperty("title", "Local");
    doc.setProperty("author", "Local");

    db.save(doc);
  }

  @Test
  public void testMetadata() {
    final ODocument index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Song.title").getMetadata();

    Assert.assertEquals(index.field("analyzer"), StandardAnalyzer.class.getName());
  }

  @Test
  public void testQueries() {
    OResultSet docs = db.query("select * from Song where search_fields(['title'],'mountain')=true");

    assertThat(docs).hasSize(4);
    docs.close();
    docs = db.query("select * from Song where search_fields(['author'],'Fabbio')=true");

    assertThat(docs).hasSize(87);
    docs.close();
    String query =
        "select * from Song where search_fields(['title'],'mountain')=true AND search_fields(['author'],'Fabbio')=true";
    docs = db.query(query);
    assertThat(docs).hasSize(1);
    docs.close();
    query =
        "select * from Song where search_fields(['title'],'mountain')=true   and author = 'Fabbio'";
    docs = db.query(query);

    assertThat(docs).hasSize(1);
    docs.close();
  }

  @Test
  public void testQeuryOnAddedDocs() {

    String query = "select * from Song where search_fields(['title'],'local')=true ";
    OResultSet docs = db.query(query);

    assertThat(docs).hasSize(1);
    docs.close();
  }
}
