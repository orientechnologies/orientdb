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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import java.util.concurrent.BlockingQueue;

/**
 * Thread that logs asynchronously.
 *
 * @author Luca Garulli
 */
public class OAuditingLoggingThread extends Thread {
  private final String databaseName;
  private final BlockingQueue<ODocument> auditingQueue;
  private volatile boolean running = true;
  private volatile boolean waitForAllLogs = true;
  private OrientDBInternal context;

  private String className;
  private OSecuritySystem security;

  public OAuditingLoggingThread(
      final String iDatabaseName,
      final BlockingQueue auditingQueue,
      final OrientDBInternal context,
      OSecuritySystem security) {
    super(
        Orient.instance().getThreadGroup(), "OrientDB Auditing Logging Thread - " + iDatabaseName);

    this.databaseName = iDatabaseName;
    this.auditingQueue = auditingQueue;
    this.context = context;
    this.security = security;
    setDaemon(true);

    // This will create a cluster in the system database for logging auditing events for
    // "databaseName", if it doesn't already
    // exist.
    // server.getSystemDatabase().createCluster(ODefaultAuditing.AUDITING_LOG_CLASSNAME,
    // ODefaultAuditing.getClusterName(databaseName));

    className = ODefaultAuditing.getClassName(databaseName);

    context
        .getSystemDatabase()
        .executeInDBScope(
            new OCallable<Void, ODatabaseSession>() {
              @Override
              public Void call(ODatabaseSession iArgument) {
                OSchema schema = iArgument.getMetadata().getSchema();
                if (!schema.existsClass(className)) {
                  OClass clazz = schema.getClass(ODefaultAuditing.AUDITING_LOG_CLASSNAME);
                  OClass cls = schema.createClass(className, clazz);
                  cls.createIndex(
                      className + ".date", OClass.INDEX_TYPE.NOTUNIQUE, new String[] {"date"});
                }
                return null;
              }
            });
  }

  @Override
  public void run() {

    while (running || waitForAllLogs) {
      try {
        if (!running && auditingQueue.isEmpty()) {
          break;
        }

        final ODocument log = auditingQueue.take();

        log.setClassName(className);

        context.getSystemDatabase().save(log);

        if (security.getSyslog() != null) {
          byte byteOp = OAuditingOperation.UNSPECIFIED.getByte();

          if (log.containsField("operation")) byteOp = log.field("operation");

          String username = log.field("user");
          String message = log.field("note");
          String dbName = log.field("database");

          security
              .getSyslog()
              .log(OAuditingOperation.getByByte(byteOp).toString(), dbName, username, message);
        }

      } catch (InterruptedException e) {
        // IGNORE AND SOFTLY EXIT

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void sendShutdown(final boolean iWaitForAllLogs) {
    this.waitForAllLogs = iWaitForAllLogs;
    running = false;
    interrupt();
  }
}
