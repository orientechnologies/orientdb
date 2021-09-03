/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SQLUpdateEdgeTest {
  private ODatabaseDocumentTx database;
  private String url;

  public SQLUpdateEdgeTest() {
    url = "memory:" + SQLUpdateEdgeTest.class.getSimpleName();

    database = new ODatabaseDocumentTx(url);
    if (database.exists()) database.open("admin", "admin");
    else database.create();
  }

  @After
  public void deinit() {
    database.close();
  }

  @Test
  public void testUpdateEdge() {

    database.command(new OCommandSQL("create class V1 extends V")).execute();

    database.command(new OCommandSQL("create class E1 extends E")).execute();

    database.getMetadata().getSchema().reload();

    // VERTEXES
    ODocument v1 = database.command(new OCommandSQL("create vertex")).execute();
    Assert.assertEquals(v1.getClassName(), OrientVertexType.CLASS_NAME);

    ODocument v2 = database.command(new OCommandSQL("create vertex V1")).execute();
    Assert.assertEquals(v2.getClassName(), "V1");

    ODocument v3 =
        database.command(new OCommandSQL("create vertex set vid = 'v3', brand = 'fiat'")).execute();
    Assert.assertEquals(v3.getClassName(), OrientVertexType.CLASS_NAME);
    Assert.assertEquals(v3.field("brand"), "fiat");

    ODocument v4 =
        database
            .command(
                new OCommandSQL("create vertex V1 set vid = 'v4',  brand = 'fiat',name = 'wow'"))
            .execute();
    Assert.assertEquals(v4.getClassName(), "V1");
    Assert.assertEquals(v4.field("brand"), "fiat");
    Assert.assertEquals(v4.field("name"), "wow");

    List<OEdge> edges =
        database
            .command(
                new OCommandSQL(
                    "create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity()))
            .execute();
    Assert.assertEquals(edges.size(), 1);
    OEdge edge = edges.get(0);
    Assert.assertEquals(edge.getSchemaType().get().getName(), "E1");

    database
        .command(
            new OCommandSQL(
                "update edge E1 set out = "
                    + v3.getIdentity()
                    + ", in = "
                    + v4.getIdentity()
                    + " where @rid = "
                    + edge.getIdentity()))
        .execute();

    List<ODocument> result =
        database.query(new OSQLSynchQuery("select expand(out('E1')) from " + v3.getIdentity()));
    Assert.assertEquals(edges.size(), 1);
    ODocument vertex4 = result.get(0);
    Assert.assertEquals(vertex4.field("vid"), "v4");

    result = database.query(new OSQLSynchQuery("select expand(in('E1')) from " + v4.getIdentity()));
    Assert.assertEquals(result.size(), 1);
    ODocument vertex3 = result.get(0);
    Assert.assertEquals(vertex3.field("vid"), "v3");

    result =
        database.query(new OSQLSynchQuery("select expand(out('E1')) from " + v1.getIdentity()));
    Assert.assertEquals(result.size(), 0);

    result = database.query(new OSQLSynchQuery("select expand(in('E1')) from " + v2.getIdentity()));
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testUpdateEdgeOfTypeE() {
    // issue #6378
    ODocument v1 = database.command(new OCommandSQL("create vertex")).execute();
    ODocument v2 = database.command(new OCommandSQL("create vertex")).execute();
    ODocument v3 = database.command(new OCommandSQL("create vertex")).execute();

    Iterable<OEdge> edges =
        database
            .command(
                new OCommandSQL(
                    "create edge E from " + v1.getIdentity() + " to " + v2.getIdentity()))
            .execute();
    OEdge edge = edges.iterator().next();

    database
        .command(
            new OCommandSQL("UPDATE EDGE " + edge.getIdentity() + " SET in = " + v3.getIdentity()))
        .execute();

    Iterable<ODocument> result =
        database
            .command(new OSQLSynchQuery<ODocument>("select expand(out()) from " + v1.getIdentity()))
            .execute();
    Assert.assertEquals(result.iterator().next().getIdentity(), v3.getIdentity());

    result =
        database
            .command(new OSQLSynchQuery<ODocument>("select expand(in()) from " + v3.getIdentity()))
            .execute();
    Assert.assertEquals(result.iterator().next().getIdentity(), v1.getIdentity());

    result =
        database
            .command(new OSQLSynchQuery<ODocument>("select expand(in()) from " + v2.getIdentity()))
            .execute();
    Assert.assertFalse(result.iterator().hasNext());
  }
}
