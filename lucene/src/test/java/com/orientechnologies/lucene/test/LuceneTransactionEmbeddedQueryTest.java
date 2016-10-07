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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class LuceneTransactionEmbeddedQueryTest {

  public LuceneTransactionEmbeddedQueryTest() {
  }

  @Test
  public void testRollback() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {
      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] { "abc" });
      db.begin();
      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\" ";
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);
      db.rollback();

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      Assert.assertEquals(vertices.size(), 0);
    } finally {
      db.drop();
    }
  }

  protected void createSchema(ODatabaseDocumentTx db) {
    final OrientVertexType c1 = new OrientGraphNoTx(db).createVertexType("C1");
    c1.createProperty("p1", OType.EMBEDDEDLIST, OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] { "p1" });
  }

  @Test
  public void txRemoveTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {
      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] { "abc" });

      OIndex<?> index = db.getMetadata().getIndexManager().getIndex("C1.p1");

      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\" ";
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

      Assert.assertEquals(index.getSize(), 1);
      db.commit();

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);
      Assert.assertEquals(index.getSize(), 1);

      db.begin();

      db.delete(vertices.get(0));

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

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

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

      Assert.assertEquals(index.getSize(), 1);
    } finally {
      db.drop();
    }
  }

  @Test
  public void txUpdateTest() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {

      OIndex<?> index = db.getMetadata().getIndexManager().getIndex("C1.p1");

      Assert.assertEquals(index.getSize(), 0);

      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] { "update removed", "update fixed" });

      db.save(doc);

      String query = "select from C1 where p1 lucene \"update\" ";
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

      Assert.assertEquals(index.getSize(), 2);

      db.commit();

      query = "select from C1 where p1 lucene \"update\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Collection coll = (Collection) index.get("update");

      Assert.assertEquals(vertices.size(), 1);
      Assert.assertEquals(coll.size(), 2);
      Assert.assertEquals(index.getSize(), 2);

      db.begin();

      //select in transaction while updating
      ODocument record = vertices.get(0);
      Collection p1 = record.field("p1");
      p1.remove("update removed");
      db.save(record);

      query = "select from C1 where p1 lucene \"update\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      coll = (Collection) index.get("update");

      Assert.assertEquals(vertices.size(), 1);
      Assert.assertEquals(coll.size(), 1);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(i, 1);

      Assert.assertEquals(index.getSize(), 1);

      query = "select from C1 where p1 lucene \"update\"";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      coll = (Collection) index.get("update");
      Assert.assertEquals(coll.size(), 1);

      Assert.assertEquals(vertices.size(), 1);

      db.rollback();

      query = "select from C1 where p1 lucene \"update\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

      Assert.assertEquals(index.getSize(), 2);
    } finally {
      db.drop();
    }

  }

  @Test
  public void txUpdateTestComplex() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {
      OIndex<?> index = db.getMetadata().getIndexManager().getIndex("C1.p1");

      Assert.assertEquals(index.getSize(), 0);

      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] { "abc" });

      ODocument doc1 = new ODocument("c1");
      doc1.field("p1", new String[] { "abc" });

      db.save(doc1);
      db.save(doc);

      db.commit();

      db.begin();

      doc.field("p1", new String[] { "removed" });
      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\"";
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
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
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      coll = (Collection) index.get("removed");

      Assert.assertEquals(vertices.size(), 1);
      Assert.assertEquals(coll.size(), 1);

      db.rollback();

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 2);

      Assert.assertEquals(index.getSize(), 2);
    } finally {
      db.drop();
    }
  }

}
