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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.types.OModifiableInteger;
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
  private volatile String                    server;
  private          Map<String, Long>         acquiredResources = new HashMap<String, Long>();

  private static final ThreadLocal<Map<String, OModifiableInteger>> acquired = ThreadLocal.withInitial(() -> {
    return new HashMap<>();
  });

  public ODistributedLockManagerRequester(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  @Override
  public void acquireExclusiveLock(final String resource, final String nodeSource, final long timeout) {
    while (true) {
      OModifiableInteger counter;
      if ((counter = acquired.get().get(resource)) != null) {
        counter.increment();
        return;
      }

      if (server == null || server.equals(manager.getLocalNodeName())) {
        // NO MASTERS, USE LOCAL SERVER
        manager.getLockManagerExecutor().acquireExclusiveLock(resource, manager.getLocalNodeName(), timeout);
        acquired.get().put(resource, new OModifiableInteger(0));
        break;
      } else {

        // SEND A DISTRIBUTED MSG TO THE LOCK MANAGER SERVER
        final Set<String> servers = new HashSet<String>();
        servers.add(server);

        ODistributedServerLog.debug(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Server '%s' is acquiring distributed lock on resource '%s'...", nodeSource, resource);

        Object result;
        try {
          final ODistributedResponse dResponse = manager
              .sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers, new ODistributedLockTask(server, resource, timeout, true),
                  manager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

          if (dResponse == null) {
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
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
          if (manager.getActiveServers().contains(server))
            // WAIT ONLY IN THE CASE THE LOCK MANAGER IS STILL ONLINE
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              break;
            }

          if (!manager.getActiveServers().contains(server)) {
            // THE LOCK MANAGER SERVER WENT DOWN DURING THE REQUEST, RETRY WITH ANOTHER LOCK MANAGER SERVER
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                "Lock Manager server '%s' went down during the request of locking resource '%s'. Waiting for the election of a new Lock Manager...",
                result, server, resource);

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

        acquired.get().put(resource, new OModifiableInteger(0));
        break;
      }
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
        "Server '%s' has acquired distributed lock on resource '%s'", nodeSource, resource);

    acquiredResources.put(resource, System.currentTimeMillis());
  }

  @Override
  public void releaseExclusiveLock(final String resource, final String nodeSource) {
    while (true) {
      OModifiableInteger counter = acquired.get().get(resource);
      if (counter == null)
        //DO NOTHING BECAUSE THE ACQUIRE DIDN'T HAPPEN IN DISTRIBUTED
        return;

      if (counter.getValue() > 0) {
        counter.decrement();
        return;
      }

      acquired.get().remove(resource);

      if (server == null || server.equals(manager.getLocalNodeName())) {
        // THE LOCK MANAGER SERVER IS THE LOCAL SERVER, RELEASE IT LOCALLY
        manager.getLockManagerExecutor().releaseExclusiveLock(resource, manager.getLocalNodeName());
        break;
      } else {
        // RELEASE THE LOCK INTO THE LOCK MANAGER SERVER SERVER
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Releasing distributed lock on resource '%s'", resource);

        final Set<String> servers = new HashSet<String>();
        servers.add(server);

        Object result;
        try {
          final ODistributedResponse dResponse = manager
              .sendRequest(OSystemDatabase.SYSTEM_DB_NAME, null, servers, new ODistributedLockTask(server, resource, 20000, false),
                  manager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

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
          if (manager.getActiveServers().contains(server))
            // WAIT ONLY IN THE CASE THE LOCK MANAGER SERVER IS STILL ONLINE
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // IGNORE IT
            }

          if (!manager.getActiveServers().contains(server)) {
            // THE LOCK MANAGER SERVER WENT DOWN DURING THE REQUEST, RETRY WITH ANOTHER LOCK MANAGER SERVER
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                "Lock Manager server '%s' went down during the request of releasing resource '%s'. Assigning new Lock Manager (error: %s)...",
                server, resource, result);

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // IGNORE IT
            }

            server = manager.getLockManagerServer();
            continue;
          }

          //Remove acquired resource anyway, the next that need to acquire the lock need to find a server first in any case.
          acquiredResources.remove(resource);
          throw (RuntimeException) result;

        } else if (result instanceof RuntimeException)
          throw (RuntimeException) result;

        break;
      }
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
        "Released distributed lock on resource '%s'", resource);

    acquiredResources.remove(resource);
  }

  @Override
  public void handleUnreachableServer(final String nodeLeftName) {
  }

  public void setServer(final String server) {
    final String lockManagerServer = this.server;
    if (lockManagerServer != null && lockManagerServer.equals(server))
      // NO CHANGES
      return;

    this.server = server;

    if (!acquiredResources.isEmpty())
      // REACQUIRE AL THE LOCKS AGAINST THE NEW LOCK MANAGER
      try {
        for (String resource : acquiredResources.keySet()) {
          acquireExclusiveLock(resource, manager.getLocalNodeName(), 20000);
        }

        // LOCKED
        ODistributedServerLog.info(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Re-acquired %d locks against the new Lock Manager server '%s'", acquiredResources.size(), server);

      } catch (OLockException e) {
        ODistributedServerLog.error(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Error on re-acquiring %d locks against the new Lock Manager '%s'", acquiredResources.size(), server);
        throw e;
      }
  }

  public String getServer() {
    return server;
  }

  @Override
  public void shutdown() {
    acquiredResources.clear();
  }
}
