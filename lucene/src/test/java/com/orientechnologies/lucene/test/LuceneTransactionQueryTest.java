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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
@Test(groups = "embedded")
public class LuceneTransactionQueryTest extends BaseConfiguredLuceneTest {

  public LuceneTransactionQueryTest() {
  }

  public LuceneTransactionQueryTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  @Override
  public void init() {
    super.init();

    final OrientVertexType c1 = new OrientGraphNoTx(databaseDocumentTx).createVertexType("C1");
    c1.createProperty("p1", OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] { "p1" });
  }

  @Test
  public void testRollback() {

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");
    databaseDocumentTx.begin();
    databaseDocumentTx.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    List<ODocument> vertices = ODatabaseRecordThreadLocal.instance().get().command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);
    databaseDocumentTx.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = ODatabaseRecordThreadLocal.instance().get().command(new OSQLSynchQuery<ODocument>(query)).execute();
    Assert.assertEquals(vertices.size(), 0);

  }

  @Test
  public void txRemoveTest() {
    databaseDocumentTx.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("C1.p1");

    databaseDocumentTx.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    List<ODocument> vertices = ODatabaseRecordThreadLocal.instance().get().command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);
    databaseDocumentTx.commit();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(index.getSize(), 1);

    databaseDocumentTx.begin();

    doc = new ODocument("c1");
    doc.field("p1", "abc");

    databaseDocumentTx.delete(vertices.get(0));

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Collection coll = (Collection) index.get("abc");

    Assert.assertEquals(vertices.size(), 0);
    Assert.assertEquals(coll.size(), 0);

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);
    Assert.assertEquals(index.getSize(), 0);

    databaseDocumentTx.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTest() {

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("C1.p1");
    OClass c1 = databaseDocumentTx.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    databaseDocumentTx.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "update");

    databaseDocumentTx.save(doc);

    String query = "select from C1 where p1 lucene \"update\" ";
    List<ODocument> vertices = ODatabaseRecordThreadLocal.instance().get().command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

    databaseDocumentTx.commit();

    query = "select from C1 where p1 lucene \"update\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Collection coll = (Collection) index.get("update");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);
    Assert.assertEquals(index.getSize(), 1);

    databaseDocumentTx.begin();

    ODocument record = vertices.get(0);
    record.field("p1", "removed");
    databaseDocumentTx.save(record);

    query = "select from C1 where p1 lucene \"update\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("update");

    Assert.assertEquals(vertices.size(), 0);
    Assert.assertEquals(coll.size(), 0);

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);

    Assert.assertEquals(index.getSize(), 1);

    query = "select from C1 where p1 lucene \"removed\"";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    databaseDocumentTx.rollback();

    query = "select from C1 where p1 lucene \"update\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTestComplex() {

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("C1.p1");
    OClass c1 = databaseDocumentTx.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    databaseDocumentTx.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");

    ODocument doc1 = new ODocument("c1");
    doc1.field("p1", "abc");

    databaseDocumentTx.save(doc1);
    databaseDocumentTx.save(doc);

    databaseDocumentTx.commit();

    databaseDocumentTx.begin();

    doc.field("p1", "removed");
    databaseDocumentTx.save(doc);

    String query = "select from C1 where p1 lucene \"abc\"";
    List<ODocument> vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    Collection coll = (Collection) index.get("abc");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    Iterator iterator = coll.iterator();
    int i = 0;
    ORecordId rid = null;
    while (iterator.hasNext()) {
      rid = (ORecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(i, 1);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getSize(), 2);

    query = "select from C1 where p1 lucene \"removed\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    databaseDocumentTx.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 2);

    Assert.assertEquals(index.getSize(), 2);

  }

}
