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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 10/08/15. */
public class LuceneTransactionQueryTest extends BaseLuceneTest {

  @Before
  public void init() {

    final OClass c1 = db.createVertexClass("C1");
    c1.createProperty("p1", OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] {"p1"});
  }

  @Test
  public void testRollback() {

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");
    db.begin();
    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    @SuppressWarnings("deprecation")
    List<ODocument> vertices =
        ODatabaseRecordThreadLocal.instance()
            .get()
            .command(new OSQLSynchQuery<ODocument>(query))
            .execute();

    Assert.assertEquals(vertices.size(), 1);
    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    //noinspection deprecation
    vertices =
        ODatabaseRecordThreadLocal.instance()
            .get()
            .command(new OSQLSynchQuery<ODocument>(query))
            .execute();
    Assert.assertEquals(vertices.size(), 0);
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    @SuppressWarnings("deprecation")
    List<ODocument> vertices =
        ODatabaseRecordThreadLocal.instance()
            .get()
            .command(new OSQLSynchQuery<ODocument>(query))
            .execute();

    Assert.assertEquals(1, vertices.size());

    Assert.assertEquals(1, index.getInternal().size());
    db.commit();

    query = "select from C1 where p1 lucene \"abc\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(1, vertices.size());
    Assert.assertEquals(1, index.getInternal().size());

    db.begin();

    doc = new ODocument("c1");
    doc.field("p1", "abc");

    db.delete(vertices.get(0));

    query = "select from C1 where p1 lucene \"abc\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Collection coll;
    try (Stream<ORID> rids = index.getInternal().getRids("abc")) {
      coll = rids.collect(Collectors.toList());
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
  }

  @Test
  public void txUpdateTest() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    OClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getInternal().size(), 0);

    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "update");

    db.save(doc);

    String query = "select from C1 where p1 lucene \"update\" ";
    @SuppressWarnings("deprecation")
    List<ODocument> vertices =
        ODatabaseRecordThreadLocal.instance()
            .get()
            .command(new OSQLSynchQuery<ODocument>(query))
            .execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getInternal().size(), 1);

    db.commit();

    query = "select from C1 where p1 lucene \"update\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Collection coll;
    try (Stream<ORID> stream = index.getInternal().getRids("update")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);
    Assert.assertEquals(index.getInternal().size(), 1);

    db.begin();

    ODocument record = vertices.get(0);
    record.field("p1", "removed");
    db.save(record);

    query = "select from C1 where p1 lucene \"update\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    try (Stream<ORID> stream = index.getInternal().getRids("update")) {
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
    Assert.assertEquals(i, 0);

    Assert.assertEquals(index.getInternal().size(), 1);

    query = "select from C1 where p1 lucene \"removed\"";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    query = "select from C1 where p1 lucene \"update\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 1);

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  @Test
  public void txUpdateTestComplex() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    OClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(index.getInternal().size(), 0);

    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");

    ODocument doc1 = new ODocument("c1");
    doc1.field("p1", "abc");

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc.field("p1", "removed");
    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\"";
    @SuppressWarnings("deprecation")
    List<ODocument> vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    Collection coll;
    try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
      coll = stream.collect(Collectors.toList());
    }

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
    Assert.assertNotNull(doc1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getInternal().size(), 2);

    query = "select from C1 where p1 lucene \"removed\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    //noinspection deprecation
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 2);

    Assert.assertEquals(index.getInternal().size(), 2);
  }
}
