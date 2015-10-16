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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
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
public class LuceneInsertUpdateTest extends BaseLuceneTest {

  public LuceneInsertUpdateTest() {
    super();
  }

  public LuceneInsertUpdateTest(boolean remote) {
    super(remote);
  }

  @Override
  protected String getDatabaseName() {
    return "insertUpdate";
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

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");

    databaseDocumentTx.save(doc);
    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 1);

    OIdentifiable next = (OIdentifiable) coll.iterator().next();
    doc = databaseDocumentTx.load(next.getRecord());
    Assert.assertEquals(doc.field("name"), "Rome");

    doc.field("name", "London");
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 1);

    next = (OIdentifiable) coll.iterator().next();
    doc = databaseDocumentTx.load(next.getRecord());
    Assert.assertEquals(doc.field("name"), "London");

    doc.field("name", "Berlin");
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("Berlin");
    Assert.assertEquals(coll.size(), 1);

  }
}
