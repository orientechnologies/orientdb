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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class OLuceneTransactionEmbeddedQueryTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    final OClass c1 = db.createVertexClass("C1");
    c1.createProperty("p1", OType.EMBEDDEDLIST, OType.STRING);
    c1.createIndex("C1.p1", "FULLTEXT", null, null, "LUCENE", new String[] { "p1" });

  }

  @Test
  public void testRollback() {

    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] { "abc" });
    db.begin();
    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true ";

    OResultSet vertices = db.command(query);
    assertThat(vertices).hasSize(1);
    db.rollback();

    query = "select from C1 where search_class( \"abc\")=true  ";
    vertices = db.command(query);
    assertThat(vertices).hasSize(0);
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] { "abc" });

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true";
    OResultSet vertices = db.command(query);

    assertThat(vertices).hasSize(1);

    Assert.assertEquals(index.getInternal().size(), 1);
    db.commit();

    vertices = db.command(query);

    assertThat(vertices).hasSize(1);
    Assert.assertEquals(index.getInternal().size(), 1);

    db.begin();

    db.delete(doc);

    vertices = db.command(query);

    Collection coll = (Collection) index.get("abc");

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

    db.rollback();

    vertices = db.command(query);

    assertThat(vertices).hasSize(1);

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  @Test
  @Ignore
  public void txUpdateTest() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(index.getInternal().size(), 0);

    db.begin();

    ODocument doc = new ODocument("c1");
    doc.field("p1", new String[] { "update removed", "update fixed" });

    db.save(doc);

    String query = "select from C1 where search_class(\"update\")=true ";
    OResultSet vertices = db.command(query);

    assertThat(vertices).hasSize(1);

    Assert.assertEquals(index.getInternal().size(), 2);

    db.commit();

    vertices = db.command(query);

    Collection coll = (Collection) index.get("update");

    assertThat(vertices).hasSize(1);
    Assert.assertEquals(coll.size(), 2);
    Assert.assertEquals(index.getInternal().size(), 2);

    db.begin();

    //select in transaction while updating
    ;
    Collection p1 = doc.field("p1");
    p1.remove("update removed");
    db.save(doc);

    vertices = db.command(query);
    coll = (Collection) index.get("update");

    assertThat(vertices).hasSize(1);
    Assert.assertEquals(coll.size(), 1);
    Assert.assertEquals(index.getInternal().size(), 1);

    vertices = db.command(query);

    coll = (Collection) index.get("update");
    Assert.assertEquals(coll.size(), 1);
    assertThat(vertices).hasSize(1);

    db.rollback();

    vertices = db.command(query);

    assertThat(vertices).hasSize(1);

    Assert.assertEquals(index.getInternal().size(), 2);

  }

  @Test
  public void txUpdateTestComplex() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(index.getInternal().size(), 0);

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
    Assert.assertEquals(index.getInternal().size(), 2);

    query = "select from C1 where p1 lucene \"removed\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();
    coll = (Collection) index.get("removed");

    Assert.assertEquals(vertices.size(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.command(new OSQLSynchQuery<ODocument>(query)).execute();

    Assert.assertEquals(vertices.size(), 2);

    Assert.assertEquals(index.getInternal().size(), 2);

  }

}
