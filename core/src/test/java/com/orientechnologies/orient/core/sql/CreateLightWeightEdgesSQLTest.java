package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateLightWeightEdgesSQLTest {

  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(CreateLightWeightEdgesSQLTest.class.getSimpleName(), ODatabaseType.MEMORY);
  }

  @Test
  public void test() {
    ODatabaseSession session = orientDB.open(CreateLightWeightEdgesSQLTest.class.getSimpleName(), "admin", "admin");

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");
    session.command("create vertex v set name='a' ");
    session.command("create vertex v set name='b' ");
    session.command("create edge e from (select from v where name='a') to (select from v where name='a') ");
    try (OResultSet res = session.query("select expand(out()) from v where name='a' ")) {
      assertEquals(res.stream().count(), 1);
    }
  }

  @After
  public void after() {
    orientDB.close();
  }

}
