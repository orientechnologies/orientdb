package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.Ignore;

/**
 * Test case to check the right management of distributed exception while a server is starting.
 * Derived from the test provided by Gino John for issue http://www.prjhub.com/#/issues/6449.
 *
 * <p>3 nodes, the test is started after the 1st node is up & running. The test is composed by
 * multiple (8) parallel threads that update the same records 20,000 times.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@Ignore
public class HARemoteGraphIT extends HALocalGraphIT {
  @Override
  protected String getDatabaseURL(final ServerRun server) {
    return "remote:localhost:2424;localhost:2425;localhost:2426/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "HARemoteGraphIT";
  }

  protected ODatabasePool getGraphFactory(final ServerRun server) {
    if (graphReadFactory == null) {
      graphReadFactory =
          new ODatabasePool(
              "remote:localhost:2424;localhost:2425;localhost:2426/",
              getDatabaseName(),
              "admin",
              "admin",
              OrientDBConfig.defaultConfig());
    }
    return graphReadFactory;
  }
}
