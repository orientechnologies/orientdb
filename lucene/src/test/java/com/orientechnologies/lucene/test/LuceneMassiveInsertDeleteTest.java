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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by enricorisa on 23/09/14.
 */
@Test(groups = "embedded")
public class LuceneMassiveInsertDeleteTest extends BaseLuceneTest {

  public LuceneMassiveInsertDeleteTest(boolean remote) {

    //super(remote);
  }

  public LuceneMassiveInsertDeleteTest() {
  }

  @Override
  protected String getDatabaseName() {
    return "massiveInsertUpdate";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("City");
    song.setSuperClass(v);
    song.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

  }

  @Test
  public void loadCloseDelete() {

    ODocument city = new ODocument("City");
    int size = 1000;
    for (int i = 0; i < size; i++) {
      city.field("name", "Rome " + i);
      databaseDocumentTx.save(city);
      city.reset();
      city.setClassName("City");
    }
    String query = "select * from City where [name] LUCENE \"(name:Rome)\"";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(docs.size(), size);

    databaseDocumentTx.close();
    databaseDocumentTx.open("admin", "admin");
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(docs.size(), size);

    databaseDocumentTx.command(new OCommandSQL("delete vertex City")).execute();

    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(docs.size(), 0);

    databaseDocumentTx.close();
    databaseDocumentTx.open("admin", "admin");
    docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(docs.size(), 0);

    databaseDocumentTx.getMetadata().reload();
    OIndex idx = databaseDocumentTx.getMetadata().getSchema().getClass("City").getClassIndex("City.name");
    idx.flush();
    Assert.assertEquals(idx.getSize(), 0);
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
