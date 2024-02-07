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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 23/09/14. */
public class LuceneMassiveInsertDeleteTest extends BaseLuceneTest {

  public LuceneMassiveInsertDeleteTest() {}

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("City");
    song.addSuperClass(v);
    song.createProperty("name", OType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void loadCloseDelete() {

    int size = 1000;
    for (int i = 0; i < size; i++) {
      ODocument city = new ODocument("City");
      city.field("name", "Rome " + i);
      db.save(city);
    }
    String query = "select * from City where name LUCENE 'name:Rome'";
    OResultSet docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), size);

    db.close();
    db = (ODatabaseDocumentInternal) openDatabase();

    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), size);

    db.command("delete vertex City").close();

    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), 0);

    db.close();
    db = (ODatabaseDocumentInternal) openDatabase();
    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), 0);

    db.getMetadata().reload();
    OIndex idx = db.getMetadata().getSchema().getClass("City").getClassIndex("City.name");
    Assert.assertEquals(idx.getInternal().size(), 0);
  }
}
