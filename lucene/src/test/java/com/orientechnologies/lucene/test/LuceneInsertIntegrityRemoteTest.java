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

import com.orientechnologies.orient.core.id.ORID;
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
// Renable when solved killing issue
@Test(groups = "remote", enabled = false)
public class LuceneInsertIntegrityRemoteTest extends BaseLuceneTest {

  public LuceneInsertIntegrityRemoteTest() {
    super();
  }

  @Override
  protected String getDatabaseName() {
    return "insertIntegrity";
  }

  @BeforeClass
  public void init() {
    initDB();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  @Test
  public void testInsertUpdateWithIndex() throws Exception {

    databaseDocumentTx.getMetadata().reload();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");

    databaseDocumentTx.begin();
    databaseDocumentTx.save(doc);
    databaseDocumentTx.commit();
    OIndex idx = schema.getClass("City").getClassIndex("City.name");

    Collection<?> coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 1);

    doc = databaseDocumentTx.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "Rome");

    databaseDocumentTx.begin();
    doc.field("name", "London");
    databaseDocumentTx.save(doc);
    databaseDocumentTx.commit();

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 1);

    doc = databaseDocumentTx.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "London");

    databaseDocumentTx.begin();
    doc.field("name", "Berlin");
    databaseDocumentTx.save(doc);
    databaseDocumentTx.commit();

    doc = databaseDocumentTx.load(doc.getIdentity(), null, true);
    Assert.assertEquals(doc.field("name"), "Berlin");

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("Berlin");
    Assert.assertEquals(idx.getSize(), 1);
    Assert.assertEquals(coll.size(), 1);

    Thread.sleep(1000);
    kill(false);

    initDB(false);

    doc = databaseDocumentTx.load(doc.getIdentity(), null, true);

    Assert.assertEquals(doc.field("name"), "Berlin");

    schema = databaseDocumentTx.getMetadata().getSchema();
    idx = schema.getClass("City").getClassIndex("City.name");

    Assert.assertEquals(idx.getSize(), 1);
    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("Berlin");
    Assert.assertEquals(coll.size(), 1);
  }
}
