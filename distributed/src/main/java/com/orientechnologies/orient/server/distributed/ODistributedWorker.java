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
package com.orientechnologies.orient.server.distributed;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedWorker extends Thread {

  protected final ODistributedDatabaseImpl                distributed;
  protected final ODistributedServerManager               manager;
  protected final ODistributedMessageServiceImpl          msgService;
  protected final String                                  localNodeName;
  protected final String                                  databaseName;
  protected final ArrayBlockingQueue<ODistributedRequest> localQueue;
  protected final int                                     id;

  protected volatile ODatabaseDocumentTx                  database;
  protected volatile OUser                                lastUser;
  protected volatile boolean                              running              = true;

  private static final long                               MAX_SHUTDOWN_TIMEOUT = 5000l;

  public ODistributedWorker(final ODistributedDatabaseImpl iDistributed, final String iDatabaseName, final int i) {
    id = i;
    setName("OrientDB DistributedWorker node=" + iDistributed.getLocalNodeName() + " db=" + iDatabaseName + " id=" + i);
    distributed = iDistributed;
    localQueue = new ArrayBlockingQueue<ODistributedRequest>(OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.getValueAsInteger());
    databaseName = iDatabaseName;
    manager = distributed.getManager();
    msgService = distributed.msgService;
    localNodeName = manager.getLocalNodeName();
  }

  public void processRequest(final ODistributedRequest request) {
    try {
      localQueue.put(request);
    } catch (InterruptedException e) {
      ODistributedServerLog.warn(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Received interruption signal, closing distributed worker thread");

      shutdown();
    }
  }

  @Override
  public void run() {
    for (long processedMessages = 0; running; processedMessages++) {
      ODistributedRequestId reqId = null;
      ODistributedRequest message = null;
      try {
        message = readRequest();

        if (message != null) {
          message.getId();
          reqId = message.getId();
          onMessage(message);
        }

      } catch (InterruptedException e) {
        // EXIT CURRENT THREAD
        Thread.currentThread().interrupt();
        break;
      } catch (DistributedObjectDestroyedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (HazelcastInstanceNotActiveException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Throwable e) {
        if (e.getCause() instanceof InterruptedException)
          Thread.currentThread().interrupt();
        else
          ODistributedServerLog.error(this, localNodeName, reqId != null ? manager.getNodeNameById(reqId.getNodeId()) : "?",
              ODistributedServerLog.DIRECTION.IN, "Error on executing distributed request %s: %s", e,
              message != null ? message.getId() : -1, message != null ? message.getTask() : "-");
      }
    }

    ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE, "end of reading requests for database %s", databaseName);
  }

  public void initDatabaseInstance() {
    // OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

    if (database == null) {
      database = distributed.getDatabaseInstance();

    } else if (database.isClosed()) {
      // DATABASE CLOSED, REOPEN IT
      manager.getServerInstance().openDatabase(database, "internal", "internal", null, true);
    }
  }

  public void shutdown() {
    running = false;

    final int pendingMsgs = localQueue.size();

    if (pendingMsgs > 0)
      ODistributedServerLog.info(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Received shutdown signal, waiting for distributed worker queue is empty (pending msgs=%d)...", pendingMsgs);

    interrupt();

    try {
      if (pendingMsgs > 0)
        try {
          join(MAX_SHUTDOWN_TIMEOUT);
        } catch (Exception e) {
          ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
              "Interrupted shutdown of distributed worker thread");
        }

      ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Shutdown distributed worker completed");

      localQueue.clear();

      if (database != null) {
        database.activateOnCurrentThread();
        database.close();
      }

    } catch (Exception e) {
      ODistributedServerLog.warn(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Error on shutting down distributed worker", e);

    }
  }

  public ODatabaseDocumentTx getDatabase() {
    return database;
  }

  protected ODistributedRequest readRequest() throws InterruptedException {
    // GET FROM DISTRIBUTED QUEUE. IF EMPTY WAIT FOR A MESSAGE
    ODistributedRequest req = nextMessage();

    if (manager.isOffline())
      waitNodeIsOnline();

    if (ODistributedServerLog.isDebugEnabled()) {
      final String senderNodeName = manager.getNodeNameById(req.getId().getNodeId());
      ODistributedServerLog.debug(this, localNodeName, senderNodeName, DIRECTION.IN, "Processing request=(%s) sourceNode=%s", req,
          senderNodeName);
    }

    return req;
  }

  protected ODistributedRequest nextMessage() throws InterruptedException {
    return localQueue.take();
  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  protected void onMessage(final ODistributedRequest iRequest) {
    final String senderNodeName = manager.getNodeNameById(iRequest.getId().getNodeId());

    final ORemoteTask task = iRequest.getTask();

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, localNodeName, senderNodeName, DIRECTION.IN, "Received request: %s", iRequest);

    // EXECUTE IT LOCALLY
    Serializable responsePayload;
    OSecurityUser origin = null;
    try {
      task.setNodeSource(senderNodeName);
      waitNodeIsOnline();
      initDatabaseInstance();

      // keep original user in database, check the username passed in request and set new user in DB, after document saved,
      // reset to original user
      if (database != null) {
        database.activateOnCurrentThread();
        origin = database.getUser();
        try {
          if (iRequest.getUserRID() != null && iRequest.getUserRID().isValid()
              && (lastUser == null || !(lastUser.getIdentity()).equals(iRequest.getUserRID()))) {
            lastUser = database.getMetadata().getSecurity().getUser(iRequest.getUserRID());
            database.setUser(lastUser);// set to new user
          } else
            origin = null;

        } catch (Throwable ex) {
          OLogManager.instance().error(this, "Failed on user switching database. " + ex.getMessage());
        }
      }

      // EXECUTE THE TASK
      for (int retry = 1;; ++retry) {
        responsePayload = manager.executeOnLocalNode(iRequest.getId(), iRequest.getTask(), database);

        if (responsePayload instanceof OModificationOperationProhibitedException) {
          // RETRY
          try {
            ODistributedServerLog.info(this, localNodeName, senderNodeName, DIRECTION.IN,
                "Database is frozen, waiting and retrying. Request %s (retry=%d, worker=%d)", iRequest, retry, id);

            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        } else {
          // OPERATION EXECUTED (OK OR ERROR), NO RETRY NEEDED
          if (retry > 1)
            ODistributedServerLog.info(this, localNodeName, senderNodeName, DIRECTION.IN, "Request %s succeed after retry=%d",
                iRequest, retry);

          break;
        }

      }

    } finally {
      if (database != null && !database.isClosed()) {
        database.activateOnCurrentThread();
        if (!database.isClosed()) {
          database.rollback();
          database.getLocalCache().clear();
          if (origin != null)
            database.setUser(origin);
        }
      }
    }

    if (running)
      sendResponseBack(iRequest, responsePayload);
  }

  protected String getLocalNodeName() {
    return localNodeName;
  }

  private void sendResponseBack(final ODistributedRequest iRequest, Serializable responsePayload) {
    if (iRequest.getId().getMessageId() < 0)
      // INTERNAL MSG
      return;

    final String senderNodeName = manager.getNodeNameById(iRequest.getId().getNodeId());

    final ODistributedResponse response = new ODistributedResponse(iRequest.getId(), localNodeName, senderNodeName,
        responsePayload);

    try {
      // GET THE SENDER'S RESPONSE QUEUE
      final ORemoteServerController remoteSenderServer = manager.getRemoteServer(senderNodeName);

      ODistributedServerLog.debug(this, localNodeName, senderNodeName, ODistributedServerLog.DIRECTION.OUT,
          "Sending response %s back", response);

      remoteSenderServer.sendResponse(response, senderNodeName);

    } catch (Exception e) {
      ODistributedServerLog.debug(this, localNodeName, senderNodeName, ODistributedServerLog.DIRECTION.OUT,
          "Error on sending response %s back", response);
    }
  }

  private void waitNodeIsOnline() throws OTimeoutException {
    // WAIT THE NODE IS ONLINE AGAIN
    final ODistributedServerManager mgr = manager.getServerInstance().getDistributedManager();
    if (mgr != null && mgr.isEnabled() && mgr.isOffline()) {
      for (int retry = 0; running; ++retry) {
        if (mgr != null && mgr.isOffline()) {
          // NODE NOT ONLINE YET, REFUSE THE CONNECTION
          OLogManager.instance().info(this,
              "Node is not online yet (status=%s), blocking the command until it is online (retry=%d, queue=%d)",
              mgr.getNodeStatus(), retry + 1, localQueue.size());

          if (localQueue.size() >= OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.getValueAsInteger()) {
            // QUEUE FULL, EMPTY THE QUEUE, IGNORE ALL THE NEXT MESSAGES UNTIL A DELTA SYNC IS EXECUTED
            ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.NONE,
                "Replication queue is full (retry=%d, queue=%d), replication could be delayed", retry + 1, localQueue.size());
          }

          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
          }
        } else
          // OK, RETURN
          return;
      }
    }
  }
}
