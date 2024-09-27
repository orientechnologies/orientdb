/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.security.auditing;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;

public class OSystemDBImporter extends Thread {
  private static final OLogger logger = OLogManager.instance().logger(OSystemDBImporter.class);
  private boolean enabled = false;
  private List<String> databaseList;
  private String auditingClass = "AuditingLog";
  private int limit = 1000; // How many records to import during each iteration.
  private int sleepPeriod = 1000; // How long to sleep (in ms) after importing 'limit' records.
  private OrientDBInternal context;
  private boolean isRunning = true;

  public boolean isEnabled() {
    return enabled;
  }

  public OSystemDBImporter(final OrientDBInternal context, final ODocument jsonConfig) {
    super(Orient.instance().getThreadGroup(), "OrientDB Auditing Log Importer Thread");

    this.context = context;

    try {
      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("databases")) {
        databaseList = (List<String>) jsonConfig.field("databases");
      }

      if (jsonConfig.containsField("limit")) {
        limit = jsonConfig.field("limit");
      }

      if (jsonConfig.containsField("sleepPeriod")) {
        sleepPeriod = jsonConfig.field("sleepPeriod");
      }
    } catch (Exception ex) {
      logger.error("OSystemDBImporter()", ex);
    }

    setDaemon(true);
  }

  public void shutdown() {
    isRunning = false;
    interrupt();
  }

  @Override
  public void run() {
    try {
      if (enabled && databaseList != null) {
        for (String dbName : databaseList) {
          if (!isRunning) break;

          importDB(dbName);
        }
      }
    } catch (Exception ex) {
      logger.error("run()", ex);
    }
  }

  private void importDB(final String dbName) {
    ODatabaseDocument db = null;
    ODatabaseDocumentInternal sysdb = null;

    try {
      db = context.openNoAuthorization(dbName);

      if (db == null) {
        logger.error("importDB() Unable to import auditing log for database: %s", null, dbName);
        return;
      }
      db.setProperty(ODefaultAuditing.IMPORTER_FLAG, true);

      sysdb = context.getSystemDatabase().openSystemDatabase();

      logger.info("Starting import of the auditing log from database: %s", dbName);

      int totalImported = 0;

      // We modify the query after the first iteration, using the last imported RID as a starting
      // point.
      String sql = String.format("select from %s order by @rid limit ?", auditingClass);

      while (isRunning) {
        db.activateOnCurrentThread();
        // Retrieve the auditing log records from the local database.
        OResultSet result = db.query(sql, limit);

        int count = 0;

        String lastRID = null;

        while (result.hasNext()) {
          OResult doc = result.next();
          try {
            OElement copy = new ODocument();

            if (doc.hasProperty("date"))
              copy.setProperty("date", doc.getProperty("date"), OType.DATETIME);

            if (doc.hasProperty("operation"))
              copy.setProperty("operation", doc.getProperty("operation"), OType.BYTE);

            if (doc.hasProperty("record"))
              copy.setProperty("record", doc.getProperty("record"), OType.LINK);

            if (doc.hasProperty("changes"))
              copy.setProperty("changes", doc.getProperty("changes"), OType.EMBEDDED);

            if (doc.hasProperty("note"))
              copy.setProperty("note", doc.getProperty("note"), OType.STRING);

            try {
              // Convert user RID to username.
              if (doc.hasProperty("user")) {
                // doc.field("user") will throw an exception if the user's ORID is not found.
                ODocument userDoc = doc.getProperty("user");
                final String username = userDoc.field("name");

                if (username != null) copy.setProperty("user", username);
              }
            } catch (Exception userEx) {
            }

            // Add the database name as part of the log stored in the system db.
            copy.setProperty("database", dbName);

            sysdb.activateOnCurrentThread();
            sysdb.save(copy, ODefaultAuditing.getClusterName(dbName));

            lastRID = doc.getIdentity().toString();

            count++;

            db.activateOnCurrentThread();
            db.delete(doc.getIdentity().get());
          } catch (Exception ex) {
            logger.error("importDB()", ex);
          }
        }

        totalImported += count;

        logger.info(
            "Imported %d auditing log %s from database: %s",
            count, count == 1 ? "record" : "records", dbName);

        Thread.sleep(sleepPeriod);

        if (lastRID != null)
          sql =
              String.format(
                  "select from %s where @rid > %s order by @rid limit ?", auditingClass, lastRID);
      }

      logger.info(
          "Completed importing of %d auditing log %s from database: %s",
          totalImported, totalImported == 1 ? "record" : "records", dbName);

    } catch (Exception ex) {
      logger.error("importDB()", ex);
    } finally {
      if (sysdb != null) {
        sysdb.activateOnCurrentThread();
        sysdb.close();
      }

      if (db != null) {
        db.activateOnCurrentThread();
        db.close();
      }
    }
  }
}
