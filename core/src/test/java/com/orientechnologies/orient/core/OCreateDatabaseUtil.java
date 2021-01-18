package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class OCreateDatabaseUtil {
  public static final String NEW_ADMIN_PASSWORD = "adminpwd";

  public static final String TYPE_PLOCAL = ODatabaseType.PLOCAL.name().toLowerCase(); // "plocal";
  public static final String TYPE_MEMORY = ODatabaseType.MEMORY.name().toLowerCase(); // "memory";

  public static OrientDB createDatabase(
      final String database, final String url, final String type) {
    final OrientDB orientDB =
        new OrientDB(
            url,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    // orientDB.create(database, ODatabaseType.PLOCAL);
    if (!orientDB.exists(database)) {
      orientDB.execute(
          "create database "
              + database
              + " "
              + type
              + " users ( admin identified by '"
              + NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    return orientDB;
  }

  public static void createDatabase(
      final String database, final OrientDB orientDB, final String type) {
    // orientDB.create(database, ODatabaseType.PLOCAL);
    if (!orientDB.exists(database)) {
      orientDB.execute(
          "create database "
              + database
              + " "
              + type
              + " users ( admin identified by '"
              + NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
  }
}
