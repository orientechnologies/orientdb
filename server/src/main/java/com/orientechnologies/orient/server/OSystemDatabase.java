/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.UUID;

public class OSystemDatabase {
  public static final String SYSTEM_DB_NAME = "OSystem";

  public static final String SERVER_INFO_CLASS  = "ServerInfo";
  public static final String SERVER_ID_PROPERTY = "serverId";

  private final OServer server;
  private       String  serverId;

  public OSystemDatabase(final OServer server) {
    this.server = server;
    init();
  }

  public String getSystemDatabaseName() {
    return OSystemDatabase.SYSTEM_DB_NAME;
  }

  public String getSystemDatabasePath() {
    return server.getDatabaseDirectory() + getSystemDatabaseName();
  }

  /**
   * Adds the specified cluster to the class, if it doesn't already exist.
   */
  public void createCluster(final String className, final String clusterName) {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      final ODatabaseDocumentInternal sysdb = openSystemDatabase();
      try {

        if (!sysdb.existsCluster(clusterName)) {
          OSchema schema = sysdb.getMetadata().getSchema();
          OClass cls = schema.getClass(className);

          if (cls != null) {
            cls.addCluster(clusterName);
          } else {
            OLogManager.instance().error(this, "createCluster() Class name %s does not exist", null, className);
          }
        }

      } finally {
        sysdb.close();
      }

    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      else
        ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  /**
   * Opens the System Database and returns an ODatabaseDocumentInternal object. The caller is responsible for retrieving any
   * ThreadLocal-stored database before openSystemDatabase() is called and restoring it after the database is closed.
   */
  public ODatabaseDocumentInternal openSystemDatabase() {
    return server.getDatabases().openNoAuthorization(getSystemDatabaseName());
  }

  public Object execute(final OCallable<Object, Object> callback, final String sql, final Object... args) {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      // BYPASS SECURITY
      final ODatabase<?> db = openSystemDatabase();
      try {
        final Object result = db.command(new OCommandSQL(sql)).execute(args);

        if (callback != null)
          return callback.call(result);
        else
          return result;

      } finally {
        db.close();
      }

    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      else
        ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  public ODocument save(final ODocument document) {
    return save(document, null);
  }

  public ODocument save(final ODocument document, final String clusterName) {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      // BYPASS SECURITY
      final ODatabaseDocumentInternal db = openSystemDatabase();
      try {
        if (clusterName != null)
          return (ODocument) db.save(document, clusterName);
        else
          return (ODocument) db.save(document);
      } finally {
        db.close();
      }

    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      else
        ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  private void init() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    final ODatabaseDocumentInternal oldDbInThread = tl != null ? tl.getIfDefined() : null;
    try {
      if (!exists()) {
        OLogManager.instance().info(this, "Creating the system database '%s' for current server", SYSTEM_DB_NAME);

        OrientDBConfig config = OrientDBConfig.builder().addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1).build();
        server.createDatabase(SYSTEM_DB_NAME, ODatabaseType.PLOCAL, config);
      }
      checkServerId();

    } finally {
      if (oldDbInThread != null) {
        ODatabaseRecordThreadLocal.instance().set(oldDbInThread);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  private synchronized void checkServerId() {
    ODatabaseDocumentInternal db = openSystemDatabase();
    try {
      OClass clazz = db.getClass(SERVER_INFO_CLASS);
      if (clazz == null) {
        clazz = db.createClass(SERVER_INFO_CLASS);
      }
      OElement info;
      if (clazz.count() == 0) {
        info = db.newElement(SERVER_INFO_CLASS);
      } else {
        info = db.browseClass(clazz.getName()).next();
      }
      this.serverId = info.getProperty(SERVER_ID_PROPERTY);
      if (this.serverId == null) {
        this.serverId = UUID.randomUUID().toString();
        info.setProperty(SERVER_ID_PROPERTY, serverId);
        info.save();
      }
    } finally {
      db.close();
    }
  }

  public void executeInDBScope(OCallable<Void, ODatabase> callback) {

    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();

    try {
      final ODatabase<?> db = openSystemDatabase();
      try {
        callback.call(db);
      } finally {
        db.close();
      }
    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      else
        ODatabaseRecordThreadLocal.instance().remove();
    }

  }

  public <T> T executeWithDB(OCallable<T, ODatabase> callback) {

    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.instance().getIfDefined();

    try {
      final ODatabase<?> db = openSystemDatabase();
      try {
        return callback.call(db);
      } finally {
        db.close();
      }
    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.instance().set(currentDB);
      else
        ODatabaseRecordThreadLocal.instance().remove();
    }

  }

  public boolean exists() {
    return server.existsDatabase(getSystemDatabaseName());
  }

  public String getServerId() {
    return serverId;
  }
}
