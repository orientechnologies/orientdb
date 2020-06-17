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
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
public class OLuceneTransactionQueryTest extends OLuceneBaseTest {

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

    String query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }

    db.rollback();

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", "abc");

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size()).isEqualTo(1);

    db.commit();

    List<OResult> results;
    try (OResultSet vertices = db.command(query)) {
      //noinspection resource
      results = vertices.stream().collect(Collectors.toList());
      assertThat(results).hasSize(1);
    }
    assertThat(index.getInternal().size()).isEqualTo(1);

    db.begin();

    doc = new ODocument("c1");
    doc.field("p1", "abc");

    //noinspection OptionalGetWithoutIsPresent
    db.delete(results.get(0).getElement().get().getIdentity());

    Collection coll;
    try (OResultSet vertices = db.query(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(coll).hasSize(0);
      assertThat(vertices).hasSize(0);
    }

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);
    assertThat(index.getInternal().size()).isEqualTo(0);
    db.rollback();

    query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size()).isEqualTo(1);
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

    String query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    Assert.assertEquals(1, index.getInternal().size());

    db.commit();

    List<OResult> results;
    try (OResultSet vertices = db.command(query)) {
      try (Stream<OResult> resultStream = vertices.stream()) {
        results = resultStream.collect(Collectors.toList());
      }
    }

    Collection coll;
    try (Stream<ORID> stream = index.getInternal().getRids("update")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(results).hasSize(1);
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size()).isEqualTo(1);

    db.begin();

    OResult record = results.get(0);
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    OElement element = record.getElement().get();
    element.setProperty("p1", "removed");
    db.save(element);

    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
    Assert.assertEquals(1, index.getInternal().size());

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
    }

    Assert.assertEquals(1, coll.size());

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("update")) {
        coll = stream.collect(Collectors.toList());
      }
      assertThat(vertices).hasSize(1);
    }
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size()).isEqualTo(1);
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

    String query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    Collection coll;
    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(1, coll.size());
    }

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

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (OResultSet vertices = db.command(query)) {
      try (Stream<ORID> stream = index.getInternal().getRids("removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 1);
    }

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    try (OResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(2);
    }

    Assert.assertEquals(index.getInternal().size(), 2);
  }
}
