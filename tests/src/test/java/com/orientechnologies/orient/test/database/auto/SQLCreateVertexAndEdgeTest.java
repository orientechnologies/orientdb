/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class SQLCreateVertexAndEdgeTest {
  private OGraphDatabase database;
  private String         url;

  @Parameters(value = "url")
  public SQLCreateVertexAndEdgeTest(String iURL) {
    url = iURL;
    database = new OGraphDatabase(iURL);
  }

  @BeforeMethod
  public void init() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void deinit() {
    database.close();
  }

  @Test
  public void createEdgeDefaultClass() {
    database.command(new OCommandSQL("create class V1 extends V")).execute();
    database.command(new OCommandSQL("create class E1 extends E")).execute();
    database.getMetadata().getSchema().reload();

    // VERTEXES
    ODocument v1 = database.command(new OCommandSQL("create vertex")).execute();
    Assert.assertEquals(v1.getClassName(), OGraphDatabase.VERTEX_CLASS_NAME);

    ODocument v2 = database.command(new OCommandSQL("create vertex V1")).execute();
    Assert.assertEquals(v2.getClassName(), "V1");

    ODocument v3 = database.command(new OCommandSQL("create vertex set brand = 'fiat'")).execute();
    Assert.assertEquals(v3.getClassName(), OGraphDatabase.VERTEX_CLASS_NAME);
    Assert.assertEquals(v3.field("brand"), "fiat");

    ODocument v4 = database.command(new OCommandSQL("create vertex V1 set brand = 'fiat',name = 'wow'")).execute();
    Assert.assertEquals(v4.getClassName(), "V1");
    Assert.assertEquals(v4.field("brand"), "fiat");
    Assert.assertEquals(v4.field("name"), "wow");

    ODocument v5 = database.command(new OCommandSQL("create vertex V1 cluster default")).execute();
    Assert.assertEquals(v5.getClassName(), "V1");
    Assert.assertEquals(v5.getIdentity().getClusterId(), database.getDefaultClusterId());

    // EDGES
    List<ODocument> edges = database.command(new OCommandSQL("create edge from " + v1.getIdentity() + " to " + v2.getIdentity()))
        .execute();
    Assert.assertFalse(edges.isEmpty());
    ODocument e1 = edges.get(0);
    Assert.assertEquals(e1.getClassName(), OGraphDatabase.EDGE_CLASS_NAME);
    Assert.assertEquals(e1.field("out"), v1);
    Assert.assertEquals(e1.field("in"), v2);

    edges = database.command(new OCommandSQL("create edge E1 from " + v1.getIdentity() + " to " + v3.getIdentity())).execute();
    Assert.assertFalse(edges.isEmpty());
    ODocument e2 = edges.get(0);
    Assert.assertEquals(e2.getClassName(), "E1");
    Assert.assertEquals(e2.field("out"), v1);
    Assert.assertEquals(e2.field("in"), v3);

    edges = database.command(
        new OCommandSQL("create edge from " + v1.getIdentity() + " to " + v4.getIdentity() + " set weight = 3")).execute();
    Assert.assertFalse(edges.isEmpty());
    ODocument e3 = edges.get(0);
    Assert.assertEquals(e3.getClassName(), OGraphDatabase.EDGE_CLASS_NAME);
    Assert.assertEquals(e3.field("out"), v1);
    Assert.assertEquals(e3.field("in"), v4);
    Assert.assertEquals(e3.field("weight"), 3);

    edges = database.command(
        new OCommandSQL("create edge E1 from " + v2.getIdentity() + " to " + v3.getIdentity() + " set weight = 10")).execute();
    Assert.assertFalse(edges.isEmpty());
    ODocument e4 = edges.get(0);
    Assert.assertEquals(e4.getClassName(), "E1");
    Assert.assertEquals(e4.field("out"), v2);
    Assert.assertEquals(e4.field("in"), v3);
    Assert.assertEquals(e4.field("weight"), 10);

    edges = database
        .command(
            new OCommandSQL("create edge e1 cluster default from " + v3.getIdentity() + " to " + v5.getIdentity()
                + " set weight = 17")).execute();
    Assert.assertFalse(edges.isEmpty());
    ODocument e5 = edges.get(0);
    Assert.assertEquals(e5.getClassName(), "E1");
    Assert.assertEquals(e5.getIdentity().getClusterId(), database.getDefaultClusterId());

    // database.command(new OCommandSQL("drop class E1")).execute();
    // database.command(new OCommandSQL("drop class V1")).execute();
  }
}
