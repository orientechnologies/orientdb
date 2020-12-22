package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class OCreateDatabaseUtil {
  static final String NEW_ADMIN_PASSWORD = "adminpwd";

  static final String TYPE_PLOCAL = "plocal";
  static final String TYPE_MEMORY = "memory";

  static OrientDB createDatabase(final String database, final String url, final String type) {
    final OrientDB orientDB =
        new OrientDB(
            url,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    // orientDB.create(database, ODatabaseType.PLOCAL);
    orientDB.execute(
        "create database "
            + database
            + " "
            + type
            + " users ( admin identified by '"
            + NEW_ADMIN_PASSWORD
            + "' role admin)");
    return orientDB;
  }
}
