package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.apache.commons.configuration.Configuration;

/** Created by Enrico Risa on 11/09/2017. */
public class OrientEmbeddedFactory {

  public static OrientStandardGraph open(final Configuration config) {
    if (config.containsKey(OrientGraph.CONFIG_DB_NAME)) {
      String dbType =
          config.getString(OrientGraph.CONFIG_DB_TYPE, ODatabaseType.PLOCAL.name()).toUpperCase();
      String dbName = config.getString(OrientGraph.CONFIG_DB_NAME);
      String user = config.getString(OrientGraph.CONFIG_USER, "admin");
      String password = config.getString(OrientGraph.CONFIG_PASS, "admin");
      boolean transactional = config.getBoolean(OrientGraph.CONFIG_TRANSACTIONAL, true);
      config.setProperty(OrientGraph.CONFIG_TRANSACTIONAL, transactional);
      OServer server = OServerMain.server();
      OrientDB context = server.getContext();
      OrientGraphFactory factory =
          new OrientGraphFactory(context, dbName, ODatabaseType.valueOf(dbType), user, password);
      return new OrientStandardGraph(factory, config);
    } else {
      return OrientFactory.open(config);
    }
  }
}
