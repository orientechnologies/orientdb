/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.security.auditing;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.server.OServer;

import java.util.concurrent.BlockingQueue;

/**
 * Thread that logs asynchronously.
 *
 * @author Luca Garulli
 */
public class OAuditingLoggingThread extends Thread {
  private final String                   databaseName;
  private final BlockingQueue<ODocument> auditingQueue;
  private volatile boolean               running        = true;
  private volatile boolean               waitForAllLogs = true;
  private OServer                        server;

  public OAuditingLoggingThread(final String iDatabaseName, final BlockingQueue auditingQueue, final OServer server) {
    super(Orient.instance().getThreadGroup(), "OrientDB Auditing Logging Thread - " + iDatabaseName);

    this.databaseName = iDatabaseName;
    this.auditingQueue = auditingQueue;
    this.server = server;
    setDaemon(true);
    
    // This will create a cluster in the system database for logging auditing events for "databaseName", if it doesn't already exist.
    server.getSystemDatabase().createCluster(ODefaultAuditing.AUDITING_LOG_CLASSNAME, ODefaultAuditing.getClusterName(databaseName));
  }

  @Override
  public void run() {

    while (running || waitForAllLogs) {
      try {
        if (!running && auditingQueue.isEmpty()) {
          break;
        }

        final ODocument log = auditingQueue.take();
        
        server.getSystemDatabase().save(log, databaseName + "_auditing");
        
        if (server.getSecurity().getSyslog() != null) {
        	 byte byteOp = OAuditingOperation.UNSPECIFIED.getByte();
        	 
        	 if(log.containsField("operation"))
        	   byteOp = log.field("operation");
        	   
        	 String username = log.field("user");
        	 String message  = log.field("note");
        	 String dbName   = log.field("database");

          server.getSecurity().getSyslog().log(OAuditingOperation.getByByte(byteOp).toString(), dbName, username, message);
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
