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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.List;

public class OSystemDBImporter extends Thread {
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
      OLogManager.instance().error(this, "OSystemDBImporter()", ex);
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
      OLogManager.instance().error(this, "run()", ex);
    }
  }

  private void importDB(final String dbName) {
    ODatabaseDocument db = null;
    ODatabaseInternal sysdb = null;

    try {
      db = context.openNoAuthorization(dbName);
      db.setProperty(ODefaultAuditing.IMPORTER_FLAG, true);

      if (db == null) {
        OLogManager.instance()
            .error(this, "importDB() Unable to import auditing log for database: %s", null, dbName);
        return;
      }

      sysdb = context.getSystemDatabase().openSystemDatabase();

      OLogManager.instance()
          .info(this, "Starting import of the auditing log from database: %s", dbName);

      int totalImported = 0;

      // We modify the query after the first iteration, using the last imported RID as a starting
      // point.
      String sql = String.format("select from %s order by @rid limit ?", auditingClass);

      while (isRunning) {
        db.activateOnCurrentThread();
        // Retrieve the auditing log records from the local database.
        List<ODocument> result = db.command(new OCommandSQL(sql)).execute(limit);

        if (result.size() == 0) break;

        int count = 0;

        String lastRID = null;

        for (ODocument doc : result) {
          try {
            ODocument copy = new ODocument();

            if (doc.containsField("date")) copy.field("date", doc.field("date"), OType.DATETIME);

            if (doc.containsField("operation"))
              copy.field("operation", doc.field("operation"), OType.BYTE);

            if (doc.containsField("record")) copy.field("record", doc.field("record"), OType.LINK);

            if (doc.containsField("changes"))
              copy.field("changes", doc.field("changes"), OType.EMBEDDED);

            if (doc.containsField("note")) copy.field("note", doc.field("note"), OType.STRING);

            try {
              // Convert user RID to username.
              if (doc.containsField("user")) {
                // doc.field("user") will throw an exception if the user's ORID is not found.
                ODocument userDoc = doc.field("user");
                final String username = userDoc.field("name");

                if (username != null) copy.field("user", username);
              }
            } catch (Exception userEx) {
            }

            // Add the database name as part of the log stored in the system db.
            copy.field("database", dbName);

            sysdb.activateOnCurrentThread();
            sysdb.save(copy, ODefaultAuditing.getClusterName(dbName));

            lastRID = doc.getIdentity().toString();

            count++;

            db.activateOnCurrentThread();
            db.delete(doc);
          } catch (Exception ex) {
            OLogManager.instance().error(this, "importDB()", ex);
          }
        }

        totalImported += count;

        OLogManager.instance()
            .info(
                this,
                "Imported %d auditing log %s from database: %s",
                count,
                count == 1 ? "record" : "records",
                dbName);

        Thread.sleep(sleepPeriod);

        if (lastRID != null)
          sql =
              String.format(
                  "select from %s where @rid > %s order by @rid limit ?", auditingClass, lastRID);
      }

      OLogManager.instance()
          .info(
              this,
              "Completed importing of %d auditing log %s from database: %s",
              totalImported,
              totalImported == 1 ? "record" : "records",
              dbName);

    } catch (Exception ex) {
      OLogManager.instance().error(this, "importDB()", ex);
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
