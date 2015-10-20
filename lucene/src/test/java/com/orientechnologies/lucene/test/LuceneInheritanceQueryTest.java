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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
@Test(groups = "embedded")
public class LuceneInheritanceQueryTest {

  public LuceneInheritanceQueryTest() {
  }

  protected void createSchema(ODatabaseDocumentTx db) {
    final OrientVertexType c1 = new OrientGraphNoTx(db).createVertexType("C1");
    c1.createProperty("name", OType.STRING);
    c1.createIndex("C1.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
    final OrientVertexType c2 = new OrientGraphNoTx(db).createVertexType("C2");
    c2.setSuperClass(c1);
  }

  @Test
  public void testQuery() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:testInheritance");
    db.create();
    createSchema(db);
    try {
      ODocument doc = new ODocument("C2");
      doc.field("name", "abc");
      db.save(doc);

      String query = "select from C1 where name lucene \"abc\" ";
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

    } finally {
      db.drop();
    }
  }

}