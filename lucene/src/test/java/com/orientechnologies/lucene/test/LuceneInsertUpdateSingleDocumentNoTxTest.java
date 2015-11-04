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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;

/**
 * Created by enricorisa on 28/06/14.
 */

@Test(groups = "embedded")
public class LuceneInsertUpdateSingleDocumentNoTxTest extends BaseLuceneTest {

  public LuceneInsertUpdateSingleDocumentNoTxTest() {
    super();
  }

  public LuceneInsertUpdateSingleDocumentNoTxTest(boolean remote) {
    super();
  }

  @Override
  protected String getDatabaseName() {
    return "insertUpdateTransaction";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    if (schema.getClass("City") == null) {
      OClass oClass = schema.createClass("City");
      oClass.createProperty("name", OType.STRING);
    }
    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() throws Exception {

    databaseDocumentTx.close();
    databaseDocumentTx.open("admin", "admin");
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    schema.reload();
    ODocument doc = new ODocument("City");
    doc.field("name", "");
    ODocument doc1 = new ODocument("City");
    doc1.field("name", "");
    doc = databaseDocumentTx.save(doc);
    doc1 = databaseDocumentTx.save(doc1);

    doc = databaseDocumentTx.load(doc);
    doc1 = databaseDocumentTx.load(doc1);
    doc.field("name", "Rome");
    doc1.field("name", "Rome");
    databaseDocumentTx.save(doc);
    databaseDocumentTx.save(doc1);
    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 2);
    Assert.assertEquals(idx.getSize(), 2);
  }
}
