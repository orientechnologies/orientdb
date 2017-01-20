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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedLockManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Distributed lock manager implementation.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedLockManagerExecutor implements ODistributedLockManager {
  private final ODistributedServerManager manager;
  private final ConcurrentHashMap<String, ODistributedLock> lockManager = new ConcurrentHashMap<String, ODistributedLock>(256);

  public ODistributedLockManagerExecutor(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  private class ODistributedLock {
    final String         server;
    final CountDownLatch lock;
    final long           acquiredOn;

    private ODistributedLock(final String server) {
      this.server = server;
      this.lock = new CountDownLatch(1);
      this.acquiredOn = System.currentTimeMillis();
    }
  }

  public void handleUnreachableServer(final String nodeLeftName) {
    final List<String> unlockedResources = new ArrayList<String>();
    for (Iterator<Map.Entry<String, ODistributedLock>> it = lockManager.entrySet().iterator(); it.hasNext(); ) {
      final Map.Entry<String, ODistributedLock> entry = it.next();

      final ODistributedLock lock = entry.getValue();

      if (lock != null && lock.server != null && lock.server.equals(nodeLeftName)) {
        OLogManager.instance().info(this, "Forcing unlocking resource '%s' acquired by '%s'", entry.getKey(), lock.server);
        unlockedResources.add(entry.getKey());
        it.remove();
      }
    }

    if (unlockedResources.size() > 0)
      ODistributedServerLog.info(this, manager.getLocalNodeName(), nodeLeftName, DIRECTION.IN,
          "Forced unlocked %d resources %s owned by server '%s'", unlockedResources.size(), unlockedResources, nodeLeftName);
  }

  public void acquireExclusiveLock(final String resource, final String nodeSource, final long timeout) {
    final ODistributedLock lock = new ODistributedLock(nodeSource);

    ODistributedLock currentLock = lockManager.putIfAbsent(resource, lock);
    if (currentLock != null) {
      if (currentLock.server.equals(nodeSource)) {
        // SAME RESOURCE/SERVER, ALREADY LOCKED
        currentLock = null;
      } else {
        // TRY TO RE-LOCK IT UNTIL TIMEOUT IS EXPIRED
        final long startTime = System.currentTimeMillis();
        do {
          try {
            ODistributedServerLog.info(this, manager.getLocalNodeName(), nodeSource, ODistributedServerLog.DIRECTION.IN,
                "Waiting to acquire distributed lock on resource '%s' (timeout=%d)...", resource, timeout);

            if (timeout > 0) {
              if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
                continue;
            } else
              currentLock.lock.await();

            currentLock = lockManager.putIfAbsent(resource, lock);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        } while (currentLock != null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout));
      }
    }

    if (currentLock != null) {
      // CHECK THE OWNER SERVER IS ONLINE. THIS AVOIDS ANY "WALKING DEAD" LOCKS
      if (currentLock.server == null || !manager.isNodeAvailable(currentLock.server)) {
        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "Forcing unlock of resource '%s' because the owner server '%s' is offline", resource, currentLock.server);

        // FORCE THE UNLOCK AND LOCK OF CURRENT REQ-ID
        lockManager.put(resource, lock);
        currentLock = null;
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      if (currentLock == null) {
        ODistributedServerLog
            .debug(this, manager.getLocalNodeName(), nodeSource, DIRECTION.IN, "Resource '%s' locked by server '%s'", resource,
                nodeSource);
      } else {
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), nodeSource, DIRECTION.IN,
            "Cannot lock resource '%s' owned by server '%s' (timeout=%d)", resource, nodeSource, timeout);
      }

    if (currentLock != null)
      throw new OLockException(
          "Cannot lock resource '" + resource + "' owned by server '" + nodeSource + "' (timeout=" + timeout + ")");
  }

  public void releaseExclusiveLock(final String resource, final String nodeSource) {
    if (resource == null)
      return;

    final ODistributedLock owner = lockManager.remove(resource);
    if (owner != null) {
      if (!owner.server.equals(nodeSource)) {
        ODistributedServerLog.error(this, manager.getLocalNodeName(), nodeSource, DIRECTION.IN,
            "Cannot unlock resource %s because owner server '%s' <> current '%s'", resource, owner.server,
            manager.getLocalNodeName());
        return;
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(this, manager.getLocalNodeName(), owner.server, DIRECTION.IN, "Unlocked resource '%s' (owner=%s elapsed=%s)",
                resource, owner.server, (System.currentTimeMillis() - owner.acquiredOn));

      // NOTIFY ANY WAITERS
      owner.lock.countDown();
    }
  }

  public void dumpLocks() {
    OLogManager.instance().info(this, "Current distributed locks for database '%s' server '%s'", manager.getLocalNodeName());
    for (Map.Entry<String, ODistributedLock> entry : lockManager.entrySet()) {
      OLogManager.instance()
          .info(this, "- %s = %s (count=%d)", entry.getKey(), entry.getValue().server, entry.getValue().lock.getCount());
    }
  }
}
