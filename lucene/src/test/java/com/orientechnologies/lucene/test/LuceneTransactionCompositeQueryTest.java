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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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
public class LuceneTransactionCompositeQueryTest extends BaseLuceneTest {

  @Before
  public void init() {

    final OClass c1 = db.createVertexClass("Foo");
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
    db.begin();
    db.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);
    db.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    Assert.assertEquals(vertices.size(), 0);

  }

  @Test
  public void txRemoveTest() {
    db.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Foo.bar");

    db.save(doc);

    db.commit();

    db.begin();

    db.delete(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

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

    db.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTest() {

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Foo.bar");
    OClass c1 = db.getMetadata().getSchema().getClass("Foo");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    db.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    db.save(doc);

    db.commit();

    db.begin();

    doc.field("bar", "removed");
    db.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
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
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getSize(), 1);

  }

  @Test
  public void txUpdateTestComplex() {

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Foo.bar");
    OClass c1 = db.getMetadata().getSchema().getClass("Foo");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getSize(), 0);

    db.begin();

    ODocument doc = new ODocument("Foo");
    doc.field("name", "Test");
    doc.field("bar", "abc");

    ODocument doc1 = new ODocument("Foo");
    doc1.field("name", "Test");
    doc1.field("bar", "abc");

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc.field("bar", "removed");
    db.save(doc);

    String query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    Collection coll = (Collection) index.get("abc");

    Assert.assertEquals(1, vertices.size());
    Assert.assertEquals(1, coll.size());

    Iterator iterator = coll.iterator();
    int i = 0;
    ORecordId rid = null;
    while (iterator.hasNext()) {
      rid = (ORecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(1, i);
    Assert.assertEquals(rid.getIdentity().toString(), doc1.getIdentity().toString());
    Assert.assertEquals(2, index.getSize());

    query = "select from Foo where name = 'Test' and bar lucene \"removed\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(1, vertices.size());
    Assert.assertEquals(1, coll.size());

    db.rollback();

    query = "select from Foo where name = 'Test' and bar lucene \"abc\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(2, vertices.size());

    Assert.assertEquals(2, index.getSize());

  }

}
