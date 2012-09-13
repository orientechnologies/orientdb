package com.orientechnologies.orient.graph.gremlin;

import java.util.Random;
import java.util.logging.Logger;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class GremlinTest {
  static Logger logger   = Logger.getLogger(GremlinTest.class.getName());

  String        url      = "remote:localhost/demo";
  String        user     = "admin";
  String        password = "admin";

  public GremlinTest() {
    OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
  }

  public ODocument setup() {
    Random r = new Random();
    OGraphDatabase gdb = OGraphDatabasePool.global().acquire(url, user, password);
    try {
      if (gdb.getVertexType("MyV") == null) {
        OClass c1 = gdb.createVertexType("MyV");
        c1.createProperty("id", OType.LONG).createIndex(INDEX_TYPE.UNIQUE);

        OClass c2 = gdb.createEdgeType("MyE");
        c2.createProperty("label", OType.STRING);
      }

      ODocument v1 = gdb.createVertex("MyV");
      v1.field("id", System.currentTimeMillis());
      v1.field("name", "parent");

      ODocument v2 = gdb.createVertex("MyV");
      v2.field("id", r.nextInt());
      v2.field("name", "child");

      ODocument e1 = gdb.createEdge(v1, v2, "MyE");
      e1.field("label", "edge");

      v1.save();
      return v2; // return the "child"
    } catch (OException e) {
      logger.severe("OrientDB Exception: " + e.toString());
    } finally {
      gdb.close();
    }
    return null;
  }

  private ODocument gremlin(OGraphDatabase gdb, String rid) {
    logger.info("run gremlin: " + gdb.hashCode());
    String req = String.format("g.v('%s').inE('%s').outV", rid, "edge");
    return gdb.command(new OCommandGremlin(req)).execute();
  }

  public void test2(String rid) {
    OGraphDatabase gdb1 = OGraphDatabasePool.global().acquire(url, user, password);
    OGraphDatabase gdb2 = OGraphDatabasePool.global().acquire(url, user, password);
    logger.info("run test2: " + gdb2.hashCode());
    try {
      ODocument doc2 = gremlin(gdb1, rid);
    } finally {
      gdb2.close();
      gdb1.close();
    }
  }

  public void test1(String rid) {
    OGraphDatabase gdb1 = OGraphDatabasePool.global().acquire(url, user, password);
    OGraphDatabase gdb2 = OGraphDatabasePool.global().acquire(url, user, password);
    logger.info("run test1: " + gdb2.hashCode());
    try {
      ODocument doc2 = gremlin(gdb1, rid);
    } finally {
      gdb1.close();
      gdb2.close();
    }
  }

  public static void main(String[] args) {
    try {
      GremlinTest g = new GremlinTest();
      ODocument doc = g.setup();

      logger.info("---- test 1 ----");
      for (int i = 0; i < 3; i++) {
        g.test1(doc.getIdentity().toString());
      }

      logger.info("---- test 2 ----");
      for (int i = 0; i < 3; i++) {
        g.test2(doc.getIdentity().toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
