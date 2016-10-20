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

import com.orientechnologies.lucene.OLuceneIndexFactory;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 09/07/15.
 */
public class LuceneManualIndexTest extends BaseLuceneTest {

  @Before
  public void init() {
    db.command(new OCommandSQL("create index manual FULLTEXT ENGINE LUCENE STRING,STRING")).execute();

    db.command(new OCommandSQL("insert into index:manual (key,rid) values(['Enrico','London'],#5:0) ")).execute();
    db.command(new OCommandSQL("insert into index:manual (key,rid) values(['Luca','Rome'],#5:0) ")).execute();
    db.command(new OCommandSQL("insert into index:manual (key,rid) values(['Luigi','Rome'],#5:0) ")).execute();

  }

  @Test
  public void shouldCreateManualIndexWithJavaApi() throws Exception {

    ODocument meta = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    OIndex<?> index = db.getMetadata().getIndexManager()
        .createIndex("apiManual", OClass.INDEX_TYPE.FULLTEXT.toString(),
            new OSimpleKeyIndexDefinition(1, OType.STRING, OType.STRING), null, null, meta, OLuceneIndexFactory.LUCENE_ALGORITHM);

    db.command(new OCommandSQL("insert into index:apiManual (key,rid) values(['Enrico','London'],#5:0) "))
        .execute();
    db.command(new OCommandSQL("insert into index:apiManual (key,rid) values(['Luca','Rome'],#5:0) ")).execute();
    db.command(new OCommandSQL("insert into index:apiManual (key,rid) values(['Luigi','Rome'],#5:0) ")).execute();

    Assert.assertEquals(index.getSize(), 3);

    List<ODocument> docs = db
        .command(new OSQLSynchQuery("select from index:apiManual where key LUCENE '(k0:Enrico)'")).execute();
    Assert.assertEquals(docs.size(), 1);

    docs = db.command(new OSQLSynchQuery("select from index:apiManual where key LUCENE '(k0:Luca)'")).execute();
    Assert.assertEquals(docs.size(), 1);

    docs = db.command(new OSQLSynchQuery("select from index:apiManual where key LUCENE '(k1:Rome)'")).execute();
    Assert.assertEquals(docs.size(), 2);

    docs = db.command(new OSQLSynchQuery("select from index:apiManual where key LUCENE '(k1:London)'")).execute();
    Assert.assertEquals(docs.size(), 1);

  }

  @Test
  public void testManualIndex() {

    OIndex<?> manual = db.getMetadata().getIndexManager().getIndex("manual");

    Assert.assertEquals(manual.getSize(), 3);

    List<ODocument> docs = db.command(new OSQLSynchQuery("select from index:manual where key LUCENE 'Enrico'"))
        .execute();
    Assert.assertEquals(docs.size(), 1);
  }

  @Test
  public void testManualIndexWitKeys() {

    OIndex<?> manual = db.getMetadata().getIndexManager().getIndex("manual");

    Assert.assertEquals(manual.getSize(), 3);

    List<ODocument> docs = db.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k0:Enrico)'"))
        .execute();
    Assert.assertEquals(docs.size(), 1);

    docs = db.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k0:Luca)'")).execute();
    Assert.assertEquals(docs.size(), 1);

    docs = db.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k1:Rome)'")).execute();
    Assert.assertEquals(docs.size(), 2);

    docs = db.command(new OSQLSynchQuery("select from index:manual where key LUCENE '(k1:London)'")).execute();
    Assert.assertEquals(docs.size(), 1);

  }

}