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
  private final ODistributedServerManager manager;
  private       String                    coordinatorServer;
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

        final ODistributedResponse dResponse = manager
            .sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers, new ODistributedLockTask(resource, timeout, true),
                manager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

        if (dResponse == null) {
          ODistributedServerLog.warn(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
              "Server '%s' cannot acquire distributed lock on resource '%s' (timeout=%d)...", nodeSource, resource, timeout);

          throw new OLockException(
              "Server '" + nodeSource + "' cannot acquire exclusive lock on resource '" + resource + "' (timeout=" + timeout + ")");
        }

        final Object result = dResponse.getPayload();

        if (result instanceof ODistributedOperationException) {
          if (manager.getActiveServers().contains(coordinatorServer))
            // WAIT ONLY IN THE CASE THE COORDINATOR IS STILL ONLINE
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              // IGNORE IT
            }

          if (!manager.getActiveServers().contains(coordinatorServer)) {
            // THE COORDINATOR WAS DOWN DURING THE REQUEST, RETRY WITH ANOTHER COORDINATOR
            coordinatorServer = manager.getCoordinatorServer();
            continue;
          }
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
    if (coordinatorServer == null || coordinatorServer.equals(manager.getLocalNodeName())) {
      // THE COORDINATOR IS THE LOCAL SERVER, RELEASE IT LOCALLY
      manager.getLockManagerExecutor().releaseExclusiveLock(resource, manager.getLocalNodeName());
    } else {
      // RELEASE THE LOCK INTO THE COORDINATOR SERVER
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
          "Releasing distributed lock on resource '%s'", resource);

      final Set<String> servers = new HashSet<String>();
      servers.add(coordinatorServer);

      final ODistributedResponse dResponse = manager
          .sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers, new ODistributedLockTask(resource, 0, false),
              manager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

      if (dResponse == null)
        throw new OLockException("Cannot release exclusive lock on resource '" + resource + "'");

      final Object result = dResponse.getPayload();
      if (result instanceof RuntimeException)
        throw (RuntimeException) result;
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
        "Released distributed lock on resource '%s'", resource);
  }

  public void setCoordinatorServer(final String coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
  }

  @Override
  public void handleUnreachableServer(final String nodeLeftName) {
    if (nodeLeftName.equals(coordinatorServer)) {
      // GET THE NEW COORDINATOR SERVER ELECTED
      coordinatorServer = manager.getCoordinatorServer();

      // REACQUIRE AL THE LOCKS AGAINST THE NEW COORDINATOR
      try {
        for (String resource : acquiredResources.keySet()) {
          acquireExclusiveLock(resource, manager.getLocalNodeName(), 10000);
        }

        // LOCKED
        ODistributedServerLog.info(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Re-acquired %d locks against the new coordinator server '%s'", acquiredResources.size(), coordinatorServer);

      } catch (OLockException e) {
        ODistributedServerLog.error(this, manager.getLocalNodeName(), coordinatorServer, ODistributedServerLog.DIRECTION.OUT,
            "Error on re-acquiring %d locks against the new coordinator '%s'", acquiredResources.size(), coordinatorServer);
      }
    }
  }
}
