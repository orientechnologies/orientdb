package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class OCreateDatabaseUtil {
  static final String NEW_ADMIN_PASSWORD = "adminpwd";

  static OrientDB createDatabase(String database, String url) {
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
            + " plocal users ( admin identified by '"
            + NEW_ADMIN_PASSWORD
            + "' role admin)");
    return orientDB;
  }
}
