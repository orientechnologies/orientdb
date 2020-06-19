package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class GremlinScriptFromOrientDBTest {

  @Test
  public void scriptMapParametersTest() {
    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseSession session = orientDB.open("test", "admin", "admin");
    session.save(session.newVertex("v"));
    OResultSet res = session.execute("gremlin", "g.V()", new HashMap<>());
    Assert.assertTrue(res.hasNext());
    session.close();
    orientDB.close();
  }

  @Test
  public void scriptArrayParametersTest() {
    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseSession session = orientDB.open("test", "admin", "admin");
    session.save(session.newVertex("v"));
    OResultSet res = session.execute("gremlin", "g.V()");
    Assert.assertTrue(res.hasNext());
    session.close();
    orientDB.close();
  }
}
