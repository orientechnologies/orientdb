package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

/**
 * Used as part of the security test refactoring of the ODB `core` module, cf.
 * https://gist.github.com/tglman/4a24fa59efd88415e765a78487d64366#file-test-migrations-md
 */
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
