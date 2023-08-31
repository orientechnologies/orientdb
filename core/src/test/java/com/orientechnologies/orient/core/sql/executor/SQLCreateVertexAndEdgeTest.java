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
package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SQLCreateVertexAndEdgeTest extends BaseMemoryDatabase {

  @Test
  public void testCreateEdgeDefaultClass() {
    int vclusterId = db.addCluster("vdefault");
    int eclusterId = db.addCluster("edefault");

    db.command("create class V1 extends V").close();
    db.command("alter class V1 addcluster vdefault").close();

    db.command("create class E1 extends E").close();
    db.command("alter class E1 addcluster edefault").close();

    db.getMetadata().getSchema().reload();

    // VERTEXES
    OVertex v1 = db.command("create vertex").next().getVertex().get();
    Assert.assertEquals(v1.getSchemaType().get().getName(), "V");

    OVertex v2 = db.command("create vertex V1").next().getVertex().get();
    Assert.assertEquals(v2.getSchemaType().get().getName(), "V1");

    OVertex v3 = db.command("create vertex set brand = 'fiat'").next().getVertex().get();
    Assert.assertEquals(v3.getSchemaType().get().getName(), "V");
    Assert.assertEquals(v3.getProperty("brand"), "fiat");

    OVertex v4 =
        db.command("create vertex V1 set brand = 'fiat',name = 'wow'").next().getVertex().get();
    Assert.assertEquals(v4.getSchemaType().get().getName(), "V1");
    Assert.assertEquals(v4.getProperty("brand"), "fiat");
    Assert.assertEquals(v4.getProperty("name"), "wow");

    OVertex v5 = db.command("create vertex V1 cluster vdefault").next().getVertex().get();
    Assert.assertEquals(v5.getSchemaType().get().getName(), "V1");
    Assert.assertEquals(v5.getIdentity().getClusterId(), vclusterId);

    // EDGES
    OResultSet edges =
        db.command("create edge from " + v1.getIdentity() + " to " + v2.getIdentity());
    assertEquals(edges.stream().count(), 1);

    edges = db.command("create edge E1 from " + v1.getIdentity() + " to " + v3.getIdentity());
    assertEquals(edges.stream().count(), 1);

    edges =
        db.command(
            "create edge from " + v1.getIdentity() + " to " + v4.getIdentity() + " set weight = 3");
    ODocument e3 = edges.next().getIdentity().get().getRecord();
    Assert.assertEquals(e3.getClassName(), "E");
    Assert.assertEquals(e3.field("out"), v1);
    Assert.assertEquals(e3.field("in"), v4);
    Assert.assertEquals(e3.<Object>field("weight"), 3);

    edges =
        db.command(
            "create edge E1 from "
                + v2.getIdentity()
                + " to "
                + v3.getIdentity()
                + " set weight = 10");
    ODocument e4 = edges.next().getIdentity().get().getRecord();
    Assert.assertEquals(e4.getClassName(), "E1");
    Assert.assertEquals(e4.field("out"), v2);
    Assert.assertEquals(e4.field("in"), v3);
    Assert.assertEquals(e4.<Object>field("weight"), 10);

    edges =
        db.command(
            "create edge e1 cluster edefault from "
                + v3.getIdentity()
                + " to "
                + v5.getIdentity()
                + " set weight = 17");
    ODocument e5 = edges.next().getIdentity().get().getRecord();
    Assert.assertEquals(e5.getClassName(), "E1");
    Assert.assertEquals(e5.getIdentity().getClusterId(), eclusterId);
  }

  /** from issue #2925 */
  @Test
  public void testSqlScriptThatCreatesEdge() {
    long start = System.currentTimeMillis();

    try {
      String cmd = "begin\n";
      cmd += "let a = create vertex set script = true\n";
      cmd += "let b = select from v limit 1\n";
      cmd += "let e = create edge from $a to $b\n";
      cmd += "commit retry 100\n";
      cmd += "return $e";

      OResultSet result = db.query("select from V");

      long before = result.stream().count();

      db.execute("sql", cmd).close();

      result = db.query("select from V");

      Assert.assertEquals(result.stream().count(), before + 1);
    } catch (Exception ex) {
      System.err.println("commit exception! " + ex);
      ex.printStackTrace(System.err);
    }

    System.out.println("done in " + (System.currentTimeMillis() - start) + "ms");
  }

  @Test
  public void testNewParser() {
    OVertex v1 = db.command("create vertex").next().getVertex().get();

    Assert.assertEquals(v1.getSchemaType().get().getName(), "V");

    ORID vid = v1.getIdentity();
    // TODO remove this

    db.command("create edge from " + vid + " to " + vid).close();

    db.command("create edge E from " + vid + " to " + vid).close();

    db.command("create edge from " + vid + " to " + vid + " set foo = 'bar'").close();

    db.command("create edge E from " + vid + " to " + vid + " set bar = 'foo'").close();
  }

  @Test
  public void testCannotAlterEClassname() {
    db.command("create class ETest extends E").close();

    try {
      db.command("alter class ETest name ETest2").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      db.command("alter class ETest name ETest2 unsafe").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(false);
    }
  }

  public void testSqlScriptThatDeletesEdge() {
    long start = System.currentTimeMillis();

    db.command("create vertex V set name = 'testSqlScriptThatDeletesEdge1'").close();
    db.command("create vertex V set name = 'testSqlScriptThatDeletesEdge2'").close();
    db.command(
            "create edge E from (select from V where name = 'testSqlScriptThatDeletesEdge1') to (select from V where name = 'testSqlScriptThatDeletesEdge2') set name = 'testSqlScriptThatDeletesEdge'")
        .close();

    try {
      String cmd = "BEGIN\n";
      cmd += "LET $groupVertices = SELECT FROM V WHERE name = 'testSqlScriptThatDeletesEdge1'\n";
      cmd += "LET $removeRoleEdge = DELETE edge E WHERE out IN $groupVertices\n";
      cmd += "COMMIT\n";
      cmd += "RETURN $groupVertices\n";

      db.execute("sql", cmd);

      OResultSet edges = db.query("select from E where name = 'testSqlScriptThatDeletesEdge'");

      Assert.assertEquals(edges.stream().count(), 0);
    } catch (Exception ex) {
      System.err.println("commit exception! " + ex);
      ex.printStackTrace(System.err);
    }

    System.out.println("done in " + (System.currentTimeMillis() - start) + "ms");
  }
}
