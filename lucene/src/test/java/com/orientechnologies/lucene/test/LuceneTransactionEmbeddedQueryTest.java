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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 10/08/15. */
public class LuceneTransactionEmbeddedQueryTest {

  public LuceneTransactionEmbeddedQueryTest() {}

  @Test
  public void testRollback() {

    @SuppressWarnings("deprecation")
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {
      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] {"abc"});
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

  private static void createSchema(ODatabaseDocumentInternal db) {
    final OClass c1 = db.createVertexClass("C1");
    c1.createProperty("p1", OType.EMBEDDEDLIST, OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] {"p1"});
  }

  @Test
  public void txRemoveTest() {
    @SuppressWarnings("deprecation")
    ODatabaseDocumentInternal db = new ODatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {
      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] {"abc"});

      OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\" ";
      @SuppressWarnings("deprecation")
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(1, vertices.size());

      Assert.assertEquals(1, index.getInternal().size());
      db.commit();

      query = "select from C1 where p1 lucene \"abc\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(1, vertices.size());
      Assert.assertEquals(1, index.getInternal().size());

      db.begin();

      db.delete(vertices.get(0));

      query = "select from C1 where p1 lucene \"abc\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Collection coll;
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.size(), 0);
      Assert.assertEquals(coll.size(), 0);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(0, i);
      Assert.assertEquals(0, index.getInternal().size());

      db.rollback();

      query = "select from C1 where p1 lucene \"abc\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(1, vertices.size());

      Assert.assertEquals(1, index.getInternal().size());
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }

  @Test
  public void txUpdateTest() {

    @SuppressWarnings("deprecation")
    ODatabaseDocumentInternal db = new ODatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {

      OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

      Assert.assertEquals(0, index.getInternal().size());

      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] {"update removed", "update fixed"});

      db.save(doc);

      String query = "select from C1 where p1 lucene \"update\" ";
      @SuppressWarnings("deprecation")
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(vertices.size(), 1);

      Assert.assertEquals(2, index.getInternal().size());

      db.commit();

      query = "select from C1 where p1 lucene \"update\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Collection coll;
      try (final Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(1, vertices.size());
      Assert.assertEquals(2, coll.size());
      Assert.assertEquals(2, index.getInternal().size());

      db.begin();

      // select in transaction while updating
      ODocument record = vertices.get(0);
      Collection p1 = record.field("p1");
      p1.remove("update removed");
      db.save(record);

      query = "select from C1 where p1 lucene \"update\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.size(), 1);
      Assert.assertEquals(coll.size(), 1);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(i, 1);

      Assert.assertEquals(1, index.getInternal().size());

      query = "select from C1 where p1 lucene \"update\"";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }
      Assert.assertEquals(coll.size(), 1);

      Assert.assertEquals(vertices.size(), 1);

      db.rollback();

      query = "select from C1 where p1 lucene \"update\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(1, vertices.size());

      Assert.assertEquals(2, index.getInternal().size());
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }

  @Test
  public void txUpdateTestComplex() {

    @SuppressWarnings("deprecation")
    ODatabaseDocumentInternal db = new ODatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {
      OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

      Assert.assertEquals(0, index.getInternal().size());

      db.begin();

      ODocument doc = new ODocument("c1");
      doc.field("p1", new String[] {"abc"});

      ODocument doc1 = new ODocument("c1");
      doc1.field("p1", new String[] {"abc"});

      db.save(doc1);
      db.save(doc);

      db.commit();

      db.begin();

      doc.field("p1", new String[] {"removed"});
      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\"";
      @SuppressWarnings("deprecation")
      List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      Collection coll;
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

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
      Assert.assertNotNull(doc1);
      Assert.assertNotNull(rid);
      Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
      Assert.assertEquals(2, index.getInternal().size());

      query = "select from C1 where p1 lucene \"removed\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
      try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(1, vertices.size());
      Assert.assertEquals(1, coll.size());

      db.rollback();

      query = "select from C1 where p1 lucene \"abc\" ";
      //noinspection deprecation
      vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

      Assert.assertEquals(2, vertices.size());

      Assert.assertEquals(2, index.getInternal().size());
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }
}
