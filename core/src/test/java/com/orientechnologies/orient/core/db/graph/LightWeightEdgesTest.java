package com.orientechnologies.orient.core.db.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LightWeightEdgesTest {

  private OrientDB orientDB;
  private ODatabaseSession session;

  @Before
  public void before() {
    // orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    // orientDB.create("test", ODatabaseType.MEMORY);
    orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    session = orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    session.command("ALTER database custom useLightweightEdges = true").close();
    session.createVertexClass("Vertex");
    session.createEdgeClass("Edge");
  }

  @Test
  public void testSimpleLightWeight() {
    OVertex v = session.newVertex("Vertex");
    OVertex v1 = session.newVertex("Vertex");
    List<OVertex> out = new ArrayList<>();
    out.add(v1);
    List<OVertex> in = new ArrayList<>();
    in.add(v);
    v.setProperty("out_Edge", out);
    v.setProperty("name", "aName");
    v1.setProperty("in_Edge", in);
    v1.setProperty("name", "bName");
    session.save(v);
    try (OResultSet res =
        session.query(" select expand(out('Edge')) from `Vertex` where name = 'aName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "bName");
    }

    try (OResultSet res =
        session.query(" select expand(in('Edge')) from `Vertex` where name = 'bName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "aName");
    }
  }

  @Test
  public void testSimpleLightWeightMissingClass() {
    OVertex v = session.newVertex("Vertex");
    OVertex v1 = session.newVertex("Vertex");
    List<OVertex> out = new ArrayList<>();
    out.add(v1);
    List<OVertex> in = new ArrayList<>();
    in.add(v);
    v.setProperty("out_Edgea", out);
    v.setProperty("name", "aName");
    v1.setProperty("in_Edgea", in);
    v1.setProperty("name", "bName");
    session.save(v);
    try (OResultSet res =
        session.query(" select expand(out('Edgea')) from `Vertex` where name = 'aName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "bName");
    }

    try (OResultSet res =
        session.query(" select expand(in('Edgea')) from `Vertex` where name = 'bName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "aName");
    }
  }

  @Test
  public void testRegularBySchema() {
    //    session.command("alter database custom useLightweightEdges = true;");
    String vClazz = "VtestRegularBySchema";
    OClass vClass = session.createVertexClass(vClazz);

    String eClazz = "EtestRegularBySchema";
    OClass eClass = session.createEdgeClass(eClazz);

    vClass.createProperty("out_" + eClazz, OType.LINKBAG, eClass);
    vClass.createProperty("in_" + eClazz, OType.LINKBAG, eClass);

    OVertex v = session.newVertex(vClass);
    v.setProperty("name", "a");
    v.save();
    OVertex v1 = session.newVertex(vClass);
    v1.setProperty("name", "b");
    v1.save();

    session.command(
        "create edge "
            + eClazz
            + " from (select from "
            + vClazz
            + " where name = 'a') to (select from "
            + vClazz
            + " where name = 'b') set name = 'foo'");

    session.execute(
        "sql",
        ""
            + "begin;"
            + "delete edge "
            + eClazz
            + ";"
            + "create edge "
            + eClazz
            + " from (select from "
            + vClazz
            + " where name = 'a') to (select from "
            + vClazz
            + " where name = 'b') set name = 'foo';"
            + "commit;");
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
  }
}
