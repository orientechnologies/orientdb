/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OSystemDatabase {
  public static final String SYSTEM_DB_NAME = "OSystem";

  private final OServer      server;

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
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      final ODatabaseDocumentInternal sysdb = openSystemDatabase();
      try {

        if (!sysdb.existsCluster(clusterName)) {
          OSchema schema = sysdb.getMetadata().getSchema();
          OClass cls = schema.getClass(className);

          if (cls != null) {
            cls.addCluster(clusterName);
          } else {
            OLogManager.instance().error(this, "createCluster() Class name %s does not exist", className);
          }
        }

      } finally {
        sysdb.close();
      }

    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.INSTANCE.set(currentDB);
      else
        ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  /**
   * Opens the System Database and returns an ODatabaseDocumentInternal object. The caller is responsible for retrieving any
   * ThreadLocal-stored database before openSystemDatabase() is called and restoring it after the database is closed.
   */
  public ODatabaseDocumentInternal openSystemDatabase() {
    return server.openDatabase(getSystemDatabaseName(), "OSuperUser", "", null, true);
  }

  public Object execute(final OCallable<Object, Object> callback, final String sql, final Object... args) {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
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
        ODatabaseRecordThreadLocal.INSTANCE.set(currentDB);
      else
        ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  public ODocument save(final ODocument document) {
    return save(document, null);
  }

  public ODocument save(final ODocument document, final String clusterName) {
    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
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
        ODatabaseRecordThreadLocal.INSTANCE.set(currentDB);
      else
        ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  private void init() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    final ODatabaseDocumentInternal oldDbInThread = tl != null ? tl.getIfDefined() : null;
    try {

      ODatabaseDocumentTx sysDB = new ODatabaseDocumentTx("plocal:" + getSystemDatabasePath());

      if (!sysDB.exists()) {
        OLogManager.instance().info(this, "Creating the system database '%s' for current server", SYSTEM_DB_NAME);

        Map<OGlobalConfiguration, Object> settings = new ConcurrentHashMap<OGlobalConfiguration, Object>();
        settings.put(OGlobalConfiguration.CREATE_DEFAULT_USERS, false);
        settings.put(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1);

        sysDB.create(settings);
        sysDB.close();
      }

    } finally {
      if (oldDbInThread != null) {
        ODatabaseRecordThreadLocal.INSTANCE.set(oldDbInThread);
      } else {
        ODatabaseRecordThreadLocal.INSTANCE.remove();
      }
    }
  }

  public void executeInDBScope(OCallable<Void, ODatabase> callback) {

    final ODatabaseDocumentInternal currentDB = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    try {
      final ODatabase<?> db = openSystemDatabase();
      try {
        callback.call(db);
      } finally {
        db.close();
      }
    } finally {
      if (currentDB != null)
        ODatabaseRecordThreadLocal.INSTANCE.set(currentDB);
      else
        ODatabaseRecordThreadLocal.INSTANCE.remove();
    }

  }

  public boolean exists() {
    final ODatabaseDocumentInternal oldDbInThread = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {

      ODatabaseDocumentTx sysDB = new ODatabaseDocumentTx("plocal:" + getSystemDatabasePath());

      return sysDB.exists();

    } finally {
      if (oldDbInThread != null) {
        ODatabaseRecordThreadLocal.INSTANCE.set(oldDbInThread);
      } else {
        ODatabaseRecordThreadLocal.INSTANCE.remove();
      }
    }
  }
}
