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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by Enrico Risa on 10/08/15. */
public class OLuceneTransactionEmbeddedQueryTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    final OClass c1 = db.createVertexClass("C1");
    c1.createProperty("p1", OType.EMBEDDEDLIST, OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] {"p1"});
  }

  @Test
  public void testRollback() {
    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] {"abc"});
    db.begin();
    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true ";

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    db.rollback();

    query = "select from C1 where search_class( \"abc\")=true  ";
    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] {"abc"});

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true";
    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(), 1);
    }
    db.commit();

    try (OResultSet vertices = db.command(query)) {

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(), 1);
    }

    db.begin();

    db.delete(doc);

    try (OResultSet vertices = db.command(query)) {

      Collection coll;
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(0);
      Assert.assertEquals(coll.size(), 0);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(i, 0);
      Assert.assertEquals(index.getInternal().size(), 0);
    }

    db.rollback();

    try (OResultSet vertices = db.command(query)) {

      assertThat(vertices).hasSize(1);

      Assert.assertEquals(index.getInternal().size(), 1);
    }
  }

  @Test
  @Ignore
  public void txUpdateTest() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(index.getInternal().size(), 0);

    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] {"update removed", "update fixed"});

    db.save(doc);

    String query = "select from C1 where search_class(\"update\")=true ";
    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(), 2);
    }
    db.commit();

    Collection coll;
    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 2);
      Assert.assertEquals(index.getInternal().size(), 2);
    }
    db.begin();

    // select in transaction while updating
    Collection p1 = doc.field("p1");
    p1.remove("update removed");
    db.save(doc);

    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(index.getInternal().size(), 1);
    }

    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }
      Assert.assertEquals(coll.size(), 1);
      assertThat(vertices).hasSize(1);
    }

    db.rollback();

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }

    Assert.assertEquals(index.getInternal().size(), 2);
  }

  @Test
  public void txUpdateTestComplex() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(index.getInternal().size(), 0);

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

    try (OResultSet vertices = db.query(query)) {
      Collection coll;
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.stream().count(), 1);
      Assert.assertEquals(coll.size(), 1);

      Iterator iterator = coll.iterator();
      int i = 0;
      ORecordId rid = null;
      while (iterator.hasNext()) {
        rid = (ORecordId) iterator.next();
        i++;
      }

      Assert.assertEquals(i, 1);
      Assert.assertNotNull(rid);
      Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
      Assert.assertEquals(index.getInternal().size(), 2);
    }

    query = "select from C1 where p1 lucene \"removed\" ";
    try (OResultSet vertices = db.query(query)) {
      Collection coll;
      try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.stream().count(), 1);
      Assert.assertEquals(coll.size(), 1);

      db.rollback();
    }

    query = "select from C1 where p1 lucene \"abc\" ";

    try (OResultSet vertices = db.query(query)) {

      Assert.assertEquals(vertices.stream().count(), 2);

      Assert.assertEquals(index.getInternal().size(), 2);
    }
  }
}
