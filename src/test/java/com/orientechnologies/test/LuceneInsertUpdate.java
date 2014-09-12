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

package com.orientechnologies.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneInsertUpdate {

  private ODatabaseDocument databaseDocumentTx;
  private static String     url;
  static {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    url = "plocal:" + buildDirectory + "/inserUpdate";

  }

  public LuceneInsertUpdate() {
    super();
  }

  @Test
  public void testInsertUpdateWithIndex() throws Exception {

    databaseDocumentTx = new ODatabaseDocumentTx(url);
    if (!url.contains("remote:") && databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
      databaseDocumentTx.create();
    } else {
      databaseDocumentTx.create();
    }

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    if (schema.getClass("City") == null) {
      OClass oClass = schema.createClass("City");

      oClass.createProperty("name", OType.STRING);
      oClass.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
    }

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");

    databaseDocumentTx.save(doc);
    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 1);

    doc = databaseDocumentTx.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "Rome");

    doc.field("name", "London");
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 1);

    doc = databaseDocumentTx.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "London");

    doc.field("name", "Berlin");
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("Berlin");
    Assert.assertEquals(coll.size(), 1);

    databaseDocumentTx.drop();
  }
}
