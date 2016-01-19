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
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IQueue;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.distributed.ODiscardedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordTask;
import com.orientechnologies.orient.server.distributed.task.ODeleteRecordTask;
import com.orientechnologies.orient.server.distributed.task.OFixTxTask;
import com.orientechnologies.orient.server.distributed.task.OResurrectRecordTask;
import com.orientechnologies.orient.server.distributed.task.OSQLCommandTask;
import com.orientechnologies.orient.server.distributed.task.OTxTask;
import com.orientechnologies.orient.server.distributed.task.OUpdateRecordTask;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedWorker extends Thread {

  private final static int                            LOCAL_QUEUE_MAXSIZE = 1000;
  protected final OHazelcastDistributedDatabase       distributed;
  protected final OHazelcastPlugin                    manager;
  protected final OHazelcastDistributedMessageService msgService;
  protected final String                              databaseName;
  protected final IQueue                              requestQueue;
  protected Queue<ODistributedRequest>                localQueue          = new ArrayBlockingQueue<ODistributedRequest>(
      LOCAL_QUEUE_MAXSIZE);
  protected volatile ODatabaseDocumentTx              database;
  protected volatile OUser                            lastUser;
  protected volatile boolean                          running             = true;

  public ODistributedWorker(final OHazelcastDistributedDatabase iDistributed, final IQueue iRequestQueue,
      final String iDatabaseName, final int i) {
    setName("OrientDB DistributedWorker node=" + iDistributed.getLocalNodeName() + " db=" + iDatabaseName + " id=" + i);
    distributed = iDistributed;
    requestQueue = iRequestQueue;
    databaseName = iDatabaseName;
    manager = distributed.manager;
    msgService = distributed.msgService;
  }

  @Override
  public void run() {
    for (long processedMessages = 0; running; processedMessages++) {
      String senderNode = null;
      ODistributedRequest message = null;
      try {
        message = readRequest();

        if (message != null) {
          message.getId();
          senderNode = message.getSenderNodeName();
          onMessage(message);
        }

      } catch (InterruptedException e) {
        // EXIT CURRENT THREAD
        Thread.interrupted();
        break;
      } catch (DistributedObjectDestroyedException e) {
        Thread.interrupted();
        break;
      } catch (HazelcastInstanceNotActiveException e) {
        Thread.interrupted();
        break;
      } catch (Throwable e) {
        if (e.getCause() instanceof InterruptedException)
          Thread.interrupted();
        else
          ODistributedServerLog.error(this, manager.getLocalNodeName(), senderNode, DIRECTION.IN,
              "error on executing distributed request %d: %s", e, message != null ? message.getId() : -1,
              message != null ? message.getTask() : "-");
      }
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.NONE, "end of reading requests for database %s",
        databaseName);
  }

  public void initDatabaseInstance() {
    if (database == null) {
      // OPEN IT
      database = (ODatabaseDocumentTx) manager.getServerInstance().openDatabase(databaseName, "internal", "internal", null, true);

      // AVOID RELOADING DB INFORMATION BECAUSE OF DEADLOCKS
      // database.reload();

    } else if (database.isClosed()) {
      // DATABASE CLOSED, REOPEN IT
      manager.getServerInstance().openDatabase(database, "internal", "internal", null, true);

      // AVOID RELOADING DB INFORMATION BECAUSE OF DEADLOCKS
      // database.reload();
    }
  }

  public void shutdown() {
    final int pendingMsgs = localQueue.size();

    if (pendingMsgs > 0)
      ODistributedServerLog.warn(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Received shutdown signal, waiting for distributed worker queue is empty (pending msgs=%d)...", pendingMsgs);

    try {
      running = false;
      interrupt();

      if (pendingMsgs > 0)
        join();

      ODistributedServerLog.warn(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Shutdown distributed worker completed");

      localQueue.clear();

      if (database != null) {
        database.activateOnCurrentThread();
        database.close();
      }

    } catch (Exception e) {
      ODistributedServerLog.warn(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error on shutting down distributed worker", e);

    }
  }

  public ODatabaseDocumentTx getDatabase() {
    return database;
  }

  protected ODistributedRequest readRequest() throws InterruptedException {
    // GET FROM DISTRIBUTED QUEUE. IF EMPTY WAIT FOR A MESSAGE
    ODistributedRequest req = nextMessage();

    while (distributed.waitForMessageId.get() > -1) {
      if (req != null) {
        if (req.getId() >= distributed.waitForMessageId.get()) {
          // ARRIVED, RESET IT
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
              "reached waited request %d on request=%s sourceNode=%s", distributed.waitForMessageId.get(), req,
              req.getSenderNodeName());

          distributed.waitForMessageId.set(-1);
          break;
        } else {
          // SKIP IT
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
              "discarded request %d because waiting for %d request=%s sourceNode=%s", req.getId(),
              distributed.waitForMessageId.get(), req, req.getSenderNodeName());

          sendResponseBack(req, req.getTask(), new ODiscardedResponse());

          // READ THE NEXT ONE
          req = nextMessage();
        }
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
          "processing request=%s sourceNode=%s", req, req.getSenderNodeName());

    return req;
  }

  protected ODistributedRequest nextMessage() throws InterruptedException {
    while (localQueue.isEmpty()) {
      // WAIT FOR THE FIRST MESSAGE
      localQueue.offer((ODistributedRequest) requestQueue.take());

      // READ MULTIPLE MSGS IN ONE SHOT BY USING LOCAL QUEUE TO IMPROVE PERFORMANCE
      requestQueue.drainTo(localQueue, LOCAL_QUEUE_MAXSIZE - 1);
    }

    return localQueue.poll();
  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  protected void onMessage(final ODistributedRequest iRequest) {
    OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

    try {
      final OAbstractRemoteTask task = iRequest.getTask();

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), iRequest.getSenderNodeName(), DIRECTION.OUT,
            "received request: %s", iRequest);

      // EXECUTE IT LOCALLY
      Serializable responsePayload;
      OSecurityUser origin = null;
      try {
        // EXECUTE THE TASK
        for (int retry = 1;; ++retry) {
          if (task.isRequiredOpenDatabase())
            initDatabaseInstance();

          database.activateOnCurrentThread();

          task.setNodeSource(iRequest.getSenderNodeName());

          // keep original user in database, check the username passed in request and set new user in DB, after document saved,
          // reset
          // to original user
          if (database != null) {
            origin = database.getUser();
            try {
              if (lastUser == null || !(lastUser.getIdentity()).equals(iRequest.getUserRID()))
                lastUser = database.getMetadata().getSecurity().getUser(iRequest.getUserRID());
              database.setUser(lastUser);// set to new user
            } catch (Throwable ex) {
              OLogManager.instance().error(this, "Failed on user switching " + ex.getMessage());
            }
          }

          responsePayload = manager.executeOnLocalNode(iRequest, database);

          if (responsePayload instanceof OModificationOperationProhibitedException) {
            OLogManager.instance().info(this,
                "Database is locked on current node (backup is running?) retrying to execute the operation (retry=%d)", retry);
            // RETRY
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
          } else
            // OPERATION EXECUTED (OK OR ERROR), NO RETRY NEEDED
            break;

        }

      } finally {
        if (database != null && !database.isClosed()) {
          database.activateOnCurrentThread();
          database.rollback();
          database.getLocalCache().clear();
          database.setUser(origin);
        }
      }

      sendResponseBack(iRequest, task, responsePayload);

    } finally {
      OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);
    }
  }

  /**
   * Composes the undo queue name based on node name.
   */
  protected String getPendingRequestMapName() {
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append(distributed.NODE_QUEUE_PREFIX);
    buffer.append(manager.getLocalNodeName());
    buffer.append(distributed.NODE_QUEUE_PENDING_POSTFIX);
    return buffer.toString();
  }

  protected String getLocalNodeName() {
    return manager.getLocalNodeName();
  }

  /**
   * Checks if last pending operation must be re-executed or not. In some circustamces the exception
   * OHotAlignmentNotPossibleException is raised because it's not possible to recover the database state.
   *
   * @throws OHotAlignmentNotPossibleException
   */
  protected void hotAlignmentError(final ODistributedRequest iLastPendingRequest, final String iMessage, final Object... iParams)
      throws OHotAlignmentNotPossibleException {
    final String msg = String.format(iMessage, iParams);

    ODistributedServerLog.warn(this, getLocalNodeName(), iLastPendingRequest.getSenderNodeName(), DIRECTION.IN, "- " + msg);
    throw new OHotAlignmentNotPossibleException(msg);
  }

  protected boolean checkIfOperationHasBeenExecuted(final ODistributedRequest lastPendingRequest, final OAbstractRemoteTask task) {
    boolean executeLastPendingRequest = false;

    // ASK FOR RECORD
    if (task instanceof ODeleteRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN DELETED YET
      executeLastPendingRequest = ((ODeleteRecordTask) task).getRid().getRecord() != null;
    } else if (task instanceof OUpdateRecordTask) {
      final ORecord rec = ((OUpdateRecordTask) task).getRid().getRecord();
      if (rec == null)
        ODistributedServerLog.warn(this, getLocalNodeName(), lastPendingRequest.getSenderNodeName(), DIRECTION.IN,
            "- cannot update deleted record %s, database could be not aligned", ((OUpdateRecordTask) task).getRid());
      else
        // EXECUTE ONLY IF VERSIONS DIFFER
        executeLastPendingRequest = rec.getVersion() != ((OUpdateRecordTask) task).getVersion();
    } else if (task instanceof OCreateRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN CREATED YET
      executeLastPendingRequest = ((OCreateRecordTask) task).getRid().getRecord() == null;
    } else if (task instanceof OSQLCommandTask) {
      if (!task.isIdempotent()) {
        hotAlignmentError(lastPendingRequest, "Not able to assure last command has been completed before last crash. Command='%s'",
            ((OSQLCommandTask) task).getPayload());
      }
    } else if (task instanceof OResurrectRecordTask) {
      if (((OResurrectRecordTask) task).getRid().getRecord() == null)
        // ALREADY DELETED: CANNOT RESTORE IT
        hotAlignmentError(lastPendingRequest, "Not able to resurrect deleted record '%s'", ((OResurrectRecordTask) task).getRid());
    } else if (task instanceof OTxTask) {
      // CHECK EACH TX ITEM IF HAS BEEN COMMITTED
      for (OAbstractRemoteTask t : ((OTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else if (task instanceof OFixTxTask) {
      // CHECK EACH FIX-TX ITEM IF HAS BEEN COMMITTED
      for (OAbstractRemoteTask t : ((OFixTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else
      hotAlignmentError(lastPendingRequest, "Not able to assure last operation has been completed before last crash. Task='%s'",
          task);
    return executeLastPendingRequest;
  }

  private void sendResponseBack(final ODistributedRequest iRequest, final OAbstractRemoteTask task, Serializable responsePayload) {
    ODistributedServerLog.debug(this, manager.getLocalNodeName(), iRequest.getSenderNodeName(), DIRECTION.OUT,
        "sending back response '%s' to request %d (%s)", responsePayload, iRequest.getId(), task);

    final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(iRequest.getId(), manager.getLocalNodeName(),
        iRequest.getSenderNodeName(), responsePayload);

    try {
      // GET THE SENDER'S RESPONSE QUEUE
      final IQueue queue = msgService
          .getQueue(OHazelcastDistributedMessageService.getResponseQueueName(iRequest.getSenderNodeName()));

      if (!queue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
        throw new ODistributedException("Timeout on dispatching response to the thread queue " + iRequest.getSenderNodeName());

    } catch (Exception e) {
      throw OException.wrapException(
          new ODistributedException("Cannot dispatch response to the thread queue " + iRequest.getSenderNodeName()), e);
    }
  }
}
