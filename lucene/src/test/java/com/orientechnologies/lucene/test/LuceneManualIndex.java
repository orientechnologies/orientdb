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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;

/**
 * Created by Enrico Risa on 09/07/15.
 */
public class LuceneManualIndex extends BaseLuceneTest {

  @Override
  protected String getDatabaseName() {
    return "manualIndex";
  }

  @BeforeClass
  public void init() {
    initDB();
    databaseDocumentTx.command(new OCommandSQL("create index manual FULLTEXT ENGINE LUCENE STRING,STRING")).execute();

    databaseDocumentTx.command(new OCommandSQL("insert into index:manual (key,rid) values(['Enrico','London'],#5:0) ")).execute();
    databaseDocumentTx.command(new OCommandSQL("insert into index:manual (key,rid) values(['Luca','Rome'],#5:0) ")).execute();
    databaseDocumentTx.command(new OCommandSQL("insert into index:manual (key,rid) values(['Luigi','Rome'],#5:0) ")).execute();

  }

  public void testManualIndex() {

    OIndex<?> manual = databaseDocumentTx.getMetadata().getIndexManager().getIndex("manual");

    Assert.assertEquals(manual.getSize(), 3);

    List<ODocument> docs = databaseDocumentTx.command(new OSQLSynchQuery("select from index:manual where key LUCENE 'Enrico'"))
        .execute();
    Assert.assertEquals(docs.size(), 1);
  }

  public void testManualIndexWitKeys() {

    OIndex<?> manual = databaseDocumentTx.getMetadata().getIndexManager().getIndex("manual");

    Assert.assertEquals(manual.getSize(), 3);

    List<ODocument> docs = databaseDocumentTx.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k0:Enrico)'"))
        .execute();
    Assert.assertEquals(docs.size(), 1);

    docs = databaseDocumentTx.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k0:Luca)'")).execute();
    Assert.assertEquals(docs.size(), 1);

    docs = databaseDocumentTx.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k1:London)'")).execute();
    Assert.assertEquals(docs.size(), 1);

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
