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

import com.orientechnologies.lucene.OLuceneIndexFactory;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 09/07/15.
 */
public class OLuceneManualIndexTest extends OLuceneBaseTest {

  @Before
  public void init() {
    db.command("create index manual FULLTEXT ENGINE LUCENE STRING,STRING");

    db.command("insert into index:manual (key,rid) values(['Enrico','London'],#5:0) ");
    db.command("insert into index:manual (key,rid) values(['Luca','Rome'],#5:0) ");
    db.command("insert into index:manual (key,rid) values(['Luigi','Rome'],#5:0) ");

  }

  @Test
  public void shouldCreateManualIndexWithJavaApi() throws Exception {

    ODocument meta = new ODocument().field("analyzer", StandardAnalyzer.class.getName());
    OIndex<?> index = db.getMetadata().getIndexManager()
        .createIndex("apiManual", OClass.INDEX_TYPE.FULLTEXT.toString(),
            new OSimpleKeyIndexDefinition(1, OType.STRING, OType.STRING), null, null, meta, OLuceneIndexFactory.LUCENE_ALGORITHM);

    db.command("insert into index:apiManual (key,rid) values(['Enrico','London'],#5:0) ");
    db.command("insert into index:apiManual (key,rid) values(['Luca','Rome'],#5:0) ");
    db.command("insert into index:apiManual (key,rid) values(['Luigi','Rome'],#5:0) ");

    Assert.assertEquals(index.getSize(), 4);

    OResultSet docs = db.query("select from  index:apiManual  where  key = 'k0:Enrico'");
    assertThat(docs).hasSize(1);
    docs.close();
    docs = db.command("select from index:apiManual where key = '(k0:Luca)'");
    assertThat(docs).hasSize(1);
    docs.close();
    docs = db.command("select from index:apiManual where key ='(k1:Rome)'");
    assertThat(docs).hasSize(2);
    docs.close();
    docs = db.command("select from index:apiManual where key ='(k1:London)'");
    assertThat(docs).hasSize(1);
    docs.close();
  }

  @Test
  public void testManualIndex() {

    OIndex<?> manual = db.getMetadata().getIndexManager().getIndex("manual");

    assertThat(manual.getSize()).isEqualTo(4);

    OResultSet docs = db.query("select from index:manual where key = 'Enrico'");

    assertThat(docs).hasSize(1);
    docs.close();
  }

  @Test
  public void testManualIndexWitKeys() {

    OIndex<?> manual = db.getMetadata().getIndexManager().getIndex("manual");

    Assert.assertEquals(manual.getSize(), 4);

    OResultSet docs = db.query("select from index:manual where key = '(k0:Enrico)'");
    assertThat(docs).hasSize(1);
    docs.close();
    docs = db.query("select from index:manual where key = '(k0:Luca)'");
    assertThat(docs).hasSize(1);
    docs.close();
    docs = db.query("select from index:manual where key = '(k1:Rome)'");
    assertThat(docs).hasSize(2);
    docs.close();
    docs = db.query("select from index:manual where key = '(k1:London)'");
    assertThat(docs).hasSize(1);
    docs.close();
  }

  @Test
  @Ignore
  public void testManualIndexInsideTransaction() throws Exception {

    // refs https://github.com/orientechnologies/orientdb/issues/7255
    OIndex<?> index = db.getMetadata()
        .getIndexManager()
        .createIndex("manualInTransaction", OClass.INDEX_TYPE.FULLTEXT.toString(),
            new OSimpleKeyIndexDefinition(1, OType.STRING), null, null, null,
            OLuceneIndexFactory.LUCENE_ALGORITHM);

    db.begin();
    ODocument document = db.newInstance();
    document.field("name", "Rob");
    db.save(document);

    index.put("Rob", document.getIdentity());
    index.flush();

    OResultSet docs = db.query("select from index:manualInTransaction where key = 'k0:rob'");

    assertThat(docs).hasSize(1);
    docs.close();
    db.commit();
  }

}