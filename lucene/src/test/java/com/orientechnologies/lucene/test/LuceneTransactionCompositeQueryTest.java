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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class LuceneTransactionCompositeQueryTest extends BaseConfiguredLuceneTest {

  public LuceneTransactionCompositeQueryTest() {
  }


  @Before
  @Override
  public void init() {
    super.init();

    final OrientVertexType c1 = new OrientGraphNoTx(databaseDocumentTx).createVertexType("Foo");
    c1.createProperty("name", OType.STRING);
    c1.createProperty("bar", OType.STRING);
    c1.createIndex("Foo.bar", "FULLTEXT", null, null, "LUCENE", new String[] { "bar" });
    c1.createIndex("Foo.name", "NOTUNIQUE", null, null, "SBTREE", new String[] { "name" });
  }

  @Test
  public void testRollback() {

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");
    databaseDocumentTx.begin();
    databaseDocumentTx.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);
    databaseDocumentTx.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    Assert.assertEquals(vertices.size(), 0);

  }

  @Test
  public void txRemoveTest() {
    databaseDocumentTx.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Foo.bar");

    databaseDocumentTx.save(doc);

    databaseDocumentTx.commit();

    databaseDocumentTx.begin();

    databaseDocumentTx.delete(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

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

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTest() {

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Foo.bar");
    OClass c1 = databaseDocumentTx.getMetadata().getSchema().getClass("Foo");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    databaseDocumentTx.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    databaseDocumentTx.save(doc);

    databaseDocumentTx.commit();

    databaseDocumentTx.begin();

    doc.field("bar", "removed");
    databaseDocumentTx.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
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

    Assert.assertEquals(index.getSize(), 1);

    query = "select from Foo where name = 'Test' and bar lucene \"removed\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    databaseDocumentTx.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTestComplex() {

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Foo.bar");
    OClass c1 = databaseDocumentTx.getMetadata().getSchema().getClass("Foo");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    databaseDocumentTx.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    ODocument doc1 = new ODocument("Foo");
    doc1.field("name", "Test");
    doc1.field("bar", "abc");

    databaseDocumentTx.save(doc1);
    databaseDocumentTx.save(doc);

    databaseDocumentTx.commit();

    databaseDocumentTx.begin();

    doc.field("bar", "removed");
    databaseDocumentTx.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
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

    query = "select from Foo where name = 'Test' and bar lucene \"removed\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    databaseDocumentTx.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = databaseDocumentTx.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 2);

    Assert.assertEquals(index.getSize(), 2);

  }

}
