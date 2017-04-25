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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.ODistributedLockTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Distributed lock manager requester. It uses the OSystem database for inter-node communications.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedLockManagerRequester implements ODistributedLockManager {
  private final    ODistributedServerManager manager;
  private volatile String                    coordinatorServer;
  private Map<String, Long> acquiredResources = new HashMap<String, Long>();

  public ODistributedLockManagerRequester(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  @Override
  public void acquireExclusiveLock(final String resource, final String nodeSource, final long timeout) {
    while (true) {
      if (coordinatorServer == null || coordinatorServer.equals(manager.getLocalNodeName())) {
        // NO MASTERS, USE LOCAL SERVER
        manager.getLockManagerExecutor().acquireExclusiveLock(resource, manager.getLocalNodeName(), timeout);
        break;
      } else {
        // SEND A DISTRIBUTED MSG TO THE COORDINATOR SERVER
        final Set<String> servers = new HashSet<String>();
        servers.add(coordinatorServer);

        ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Server '%s' is acquiring distributed lock on resource '%s'...", nodeSource, resource);

        Object result;
        try {
          final ODistributedResponse dResponse = manager.sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers,
              new ODistributedLockTask(coordinatorServer, resource, timeout, true), manager.getNextMessageIdCounter(),
              ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

          if (dResponse == null) {
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
                "Server '%s' cannot acquire distributed lock on resource '%s' (timeout=%d)...", nodeSource, resource, timeout);

            throw new OLockException(
                "Server '" + nodeSource + "' cannot acquire exclusive lock on resource '" + resource + "' (timeout=" + timeout
                    + ")");
          }
          result = dResponse.getPayload();
        } catch (ODistributedException e) {
          result = e;
        }

        final boolean distribException =
            result instanceof ODistributedOperationException || result instanceof ODistributedException;

        if (distribException) {
          if (manager.getActiveServers().contains(coordinatorServer))
            // WAIT ONLY IN THE CASE THE COORDINATOR IS STILL ONLINE
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              break;
            }

          if (!manager.getActiveServers().contains(coordinatorServer)) {
            // THE COORDINATOR WENT DOWN DURING THE REQUEST, RETRY WITH ANOTHER COORDINATOR
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
                "Coordinator server '%s' went down during the request of locking resource '%s'. Waiting for the election of a new coordinator...",
                coordinatorServer, resource);

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              break;
            }

            continue;
          }

          throw (RuntimeException) result;

        } else if (result instanceof RuntimeException)
          throw (RuntimeException) result;

        break;
      }
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
        "Server '%s' has acquired distributed lock on resource '%s'", nodeSource, resource);

    acquiredResources.put(resource, System.currentTimeMillis());
  }

  @Override
  public void releaseExclusiveLock(final String resource, final String nodeSource) {
    while (true) {
      if (coordinatorServer == null || coordinatorServer.equals(manager.getLocalNodeName())) {
        // THE COORDINATOR IS THE LOCAL SERVER, RELEASE IT LOCALLY
        manager.getLockManagerExecutor().releaseExclusiveLock(resource, manager.getLocalNodeName());
        break;
      } else {
        // RELEASE THE LOCK INTO THE COORDINATOR SERVER
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Releasing distributed lock on resource '%s'", resource);

        final Set<String> servers = new HashSet<String>();
        servers.add(coordinatorServer);

        Object result;
        try {
          final ODistributedResponse dResponse = manager.sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers,
              new ODistributedLockTask(coordinatorServer, resource, 20000, false), manager.getNextMessageIdCounter(),
              ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

          if (dResponse == null)
            throw new OLockException("Cannot release exclusive lock on resource '" + resource + "'");

          result = dResponse.getPayload();
        } catch (ODistributedException e) {
          result = e;
        }

        if (manager.getNodeStatus() == ODistributedServerManager.NODE_STATUS.OFFLINE)
          throw new ODistributedException("Server is OFFLINE");

        final boolean distribException =
            result instanceof ODistributedOperationException || result instanceof ODistributedException;

        if (distribException) {
          if (manager.getActiveServers().contains(coordinatorServer))
            // WAIT ONLY IN THE CASE THE COORDINATOR IS STILL ONLINE
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // IGNORE IT
            }

          if (!manager.getActiveServers().contains(coordinatorServer)) {
            // THE COORDINATOR WENT DOWN DURING THE REQUEST, RETRY WITH ANOTHER COORDINATOR
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
                "Coordinator server '%s' went down during the request of releasing resource '%s'. Assigning new coordinator (error: %s)...",
                coordinatorServer, resource, result);

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // IGNORE IT
            }

            coordinatorServer = manager.getCoordinatorServer();
            continue;
          }

          throw (RuntimeException) result;

        } else if (result instanceof RuntimeException)
          throw (RuntimeException) result;

        break;
      }
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
        "Released distributed lock on resource '%s'", resource);

    acquiredResources.remove(resource);
  }

  @Override
  public void handleUnreachableServer(final String nodeLeftName) {
  }

  public void setCoordinatorServer(final String coordinatorServer) {
    final String currentCoordinator = this.coordinatorServer;
    if (currentCoordinator != null && currentCoordinator.equals(coordinatorServer))
      // NO CHANGES
      return;

    this.coordinatorServer = coordinatorServer;

    if (!acquiredResources.isEmpty())
      // REACQUIRE AL THE LOCKS AGAINST THE NEW COORDINATOR
      try {
        for (String resource : acquiredResources.keySet()) {
          acquireExclusiveLock(resource, manager.getLocalNodeName(), 20000);
        }

        // LOCKED
        ODistributedServerLog.info(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Re-acquired %d locks against the new coordinator server '%s'", acquiredResources.size(), coordinatorServer);

      } catch (OLockException e) {
        ODistributedServerLog.error(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Error on re-acquiring %d locks against the new coordinator '%s'", acquiredResources.size(), coordinatorServer);
        throw e;
      }
  }

  public String getCoordinatorServer() {
    return coordinatorServer;
  }

  @Override
  public void shutdown() {
    acquiredResources.clear();
  }
}
