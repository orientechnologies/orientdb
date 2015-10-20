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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;

/**
 * Created by Enrico Risa on 29/04/15.
 */
public class LuceneBooleanIndex extends BaseLuceneTest {

  @Override
  protected String getDatabaseName() {
    return "booleanTest";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Person");
    song.setSuperClass(v);
    song.createProperty("isDeleted", OType.BOOLEAN);

    databaseDocumentTx.command(new OCommandSQL("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE"))
        .execute();

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  public void insertPerson() {

    for (int i = 0; i < 1000; i++) {
      ODocument doc = new ODocument("Person");
      doc.field("isDeleted", i % 2 == 0);
      databaseDocumentTx.save(doc);
    }

    List<ODocument> docs = databaseDocumentTx
        .query(new OSQLSynchQuery<ODocument>("select from Person where isDeleted lucene false"));

    Assert.assertEquals(500, docs.size());
    Assert.assertEquals(false, docs.get(0).field("isDeleted"));
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select from Person where isDeleted lucene true"));

    Assert.assertEquals(500, docs.size());
    Assert.assertEquals(true, docs.get(0).field("isDeleted"));
  }
}
