package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Iterator;
import org.testng.annotations.Test;

public class TraversalTest {
  private static final int SIZE = 1000000;
  private OrientDB orientdb =
      new OrientDB("embedded:./target/databases", OrientDBConfig.defaultConfig());

  public static void main(String[] args) {
    new TraversalTest().test();
  }

  @Test
  public void test() {
    if (orientdb.exists("test")) {
      orientdb.drop("test");
    }
    orientdb.execute(
        "create database " + "test" + " plocal users ( admin identified by 'admin' role admin)");

    ODatabaseSession db = orientdb.open("test", "admin", "admin");

    long begin = System.currentTimeMillis();
    ORID firstRid = null;
    ORID lastRid = null;
    for (int i = 0; i < SIZE; i++) {
      OVertex v = db.newVertex();
      v.setProperty("val", i);
      v.setProperty("name", "vertex" + i);
      v.setProperty("foo", "foo laksjdf lakjsdf lkasjf dlkafdjs " + i);
      v.setProperty(
          "bar", "foo adfakbjk lkjaw elkm,nbn apoij w.e,jr ;kjhaw erlkasjf dlkafdjs " + i);
      v.setProperty(
          "baz", "foo laksjdf lakjsdf .lkau s;olknawe; oih;na ero;ij; lkasjf dlkafdjs " + i);
      v.save();

      if (lastRid != null) {
        OVertex lastV = db.load(lastRid);
        OEdge edge = lastV.addEdge(v);
        edge.save();
      } else {
        firstRid = v.getIdentity();
      }

      lastRid = v.getIdentity();
    }

    System.out.println("insert in " + (System.currentTimeMillis() - begin));
    db.close();

    for (int i = 0; i < 10; i++) {
      traversal(firstRid);
    }
  }

  private void traversal(ORID firstRid) {
    ODatabaseSession db;
    long traverseTime;
    db = orientdb.open("test", "admin", "admin");
    long begin = System.currentTimeMillis();

    long tot = 0;
    OVertex v = db.load(firstRid);
    tot += ((Integer) v.getProperty("val"));

    Iterator<OVertex> vertices = v.getVertices(ODirection.OUT).iterator();

    while (vertices.hasNext()) {
      v = vertices.next();
      tot += ((Integer) v.getProperty("val"));
      vertices = v.getVertices(ODirection.OUT).iterator();
    }

    traverseTime = (System.currentTimeMillis() - begin);
    System.out.println("---");
    System.out.println("traverse in " + traverseTime);
    System.out.println("traverse microsec per vertex: " + (traverseTime * 1000 / SIZE));
    System.out.println("sum: " + tot);

    db.close();
  }
}
