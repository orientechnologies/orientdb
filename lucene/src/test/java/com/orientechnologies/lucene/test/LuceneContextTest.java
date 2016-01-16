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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Created by enricorisa on 08/10/14.
 */
public class LuceneContextTest extends BaseLuceneTest {

  @Test
  public void testContext() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    databaseDocumentTx.command(new OCommandScript("sql", getScriptFromStream(stream))).execute();

    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select *,$score from Song where [title] LUCENE \"(title:man)\""));

    Assert.assertEquals(docs.size(), 14);

    Float latestScore = 100f;
    for (ODocument doc : docs) {
      Float score = doc.field("$score");
      Assert.assertNotNull(score);
      Assert.assertTrue(score <= latestScore);
      latestScore = score;
    }

    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(
        "select *,$totalHits,$Song_title_totalHits from Song where [title] LUCENE \"(title:man)\" limit 1"));
    Assert.assertEquals(docs.size(), 1);

    ODocument doc = docs.iterator().next();
    Assert.assertEquals(doc.field("$totalHits"), 14);
    Assert.assertEquals(doc.field("$Song_title_totalHits"), 14);

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

    databaseDocumentTx.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();
    databaseDocumentTx.command(new OCommandSQL("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE")).execute();

  }

  @After
  public void deInit() {
    deInitDB();
  }

}
