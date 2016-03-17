/*
 *
 *  * Copyright 2016 OrientDB.
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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 17/03/16.
 */

@Test(groups = "embedded")
public class LuceneRebuildTest {

  public void rebuildTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:rebuildTest");

    db.create();

    try {
      OClass person = db.getMetadata().getSchema().createClass("Person");
      person.createProperty("name", OType.STRING);

      db.command(new OCommandSQL("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE")).execute();

      for (int i = 0; i < 10; i++) {
        ODocument doc = new ODocument("Person");
        doc.field("name", "John");
        db.save(doc);
      }

      OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Person.name");

      index.rebuild();

      List<ODocument> results = db.command(new OCommandSQL("select from Person where name lucene 'John'")).execute();

      Assert.assertEquals(results.size(), 10);

    } finally {
      db.drop();
    }
  }
}
