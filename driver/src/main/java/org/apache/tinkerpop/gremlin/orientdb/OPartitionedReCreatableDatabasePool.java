package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import java.util.Optional;

public class OPartitionedReCreatableDatabasePool {
  private final OrientDB orientdb;
  private final Optional<ODatabaseType> type;
  private ODatabasePool pool;
  private final String dbName;
  private final String userName;
  private final String password;
  private final int maxSize;

  public OPartitionedReCreatableDatabasePool(
      OrientDB orientdb, String dbName, String userName, String password, int maxSize) {
    this(orientdb, dbName, Optional.empty(), userName, password, maxSize);
  }

  public OPartitionedReCreatableDatabasePool(
      OrientDB orientdb,
      String dbName,
      Optional<ODatabaseType> type,
      String userName,
      String password,
      int maxSize) {
    this.orientdb = orientdb;
    this.dbName = dbName;
    this.type = type;
    this.userName = userName;
    this.password = password;
    this.maxSize = maxSize;

    reCreatePool();
  }

  public void reCreatePool() {
    close();
    OrientDBConfig config = OrientDBConfig.defaultConfig();
    config.getConfigurations().setValue(OGlobalConfiguration.DB_POOL_MAX, this.maxSize);

    try {
      if (this.type.isPresent() && !this.orientdb.exists(dbName)) {
        this.orientdb
            .execute(
                "create database  ? "
                    + type.get()
                    + " if not exists users( ? identified by ? role admin)",
                dbName,
                userName,
                password)
            .close();
      }
    } catch (Exception e) {

    }
    this.pool = new ODatabasePool(this.orientdb, this.dbName, this.userName, this.password, config);
  }

  public void close() {
    if (this.pool != null) this.pool.close();

    this.pool = null;
  }

  public ODatabaseDocument acquire() {
    if (this.pool != null) return this.pool.acquire();
    return null;
  }
}
