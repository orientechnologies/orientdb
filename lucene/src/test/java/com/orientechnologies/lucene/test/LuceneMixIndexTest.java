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
import java.io.InputStream;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 02/09/15. */
public class LuceneMixIndexTest extends BaseLuceneTest {

  @Before
  public void initLocal() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    db.command(new OCommandSQL("create index Song.author on Song (author) NOTUNIQUE")).execute();

    db.command(
            new OCommandSQL(
                "create index Song.composite on Song (title,lyrics) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @Test
  public void testMixQuery() {

    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where  author = 'Hornsby' and [title,lyrics]  LUCENE \"(title:mountain)\" "));

    Assert.assertEquals(1, docs.size());

    docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE \"(title:mountain)\" "));

    Assert.assertEquals(1, docs.size());

    docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE \"(title:ballad)\" "));
    Assert.assertEquals(0, docs.size());

    docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE \"(title:ballad)\" "));
    Assert.assertEquals(0, docs.size());
  }

  @Test
  //  @Ignore
  public void testMixCompositeQuery() {

    List<ODocument> docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE \"title:mountain\" "));

    Assert.assertEquals(1, docs.size());

    docs =
        db.query(
            new OSQLSynchQuery<ODocument>(
                "select * from Song where author = 'Hornsby' and [title,lyrics] LUCENE \"lyrics:happy\" "));

    Assert.assertEquals(1, docs.size());

    // docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
    // "select * from Song where  author = 'Hornsby' and [title] LUCENE \"(title:ballad)\" "));
    // Assert.assertEquals(docs.size(), 0);
    //
    // docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
    // "select * from Song where  author = 'Hornsby' and title LUCENE \"(title:ballad)\" "));
    // Assert.assertEquals(docs.size(), 0);

  }
}
