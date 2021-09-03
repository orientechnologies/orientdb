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
package com.orientechnologies.common.concur.resource;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OAcquireTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic non reentrant implementation about pool of resources. It pre-allocates a semaphore of
 * maxResources. Resources are lazily created by invoking the listener.
 *
 * @param <K> Resource's Key
 * @param <V> Resource Object
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OResourcePool<K, V> {
  protected final Semaphore sem;
  protected final Queue<V> resources = new ConcurrentLinkedQueue<V>();
  protected final Queue<V> resourcesOut = new ConcurrentLinkedQueue<V>();
  protected final Collection<V> unmodifiableresources;
  private final int maxResources;
  protected OResourcePoolListener<K, V> listener;
  protected final AtomicInteger created = new AtomicInteger();

  public OResourcePool(final int max, final OResourcePoolListener<K, V> listener) {
    this(0, max, listener);
  }

  public OResourcePool(int min, int max, OResourcePoolListener<K, V> listener) {
    maxResources = max;
    if (maxResources < 1) throw new IllegalArgumentException("iMaxResource must be major than 0");

    this.listener = listener;
    sem = new Semaphore(maxResources, true);
    unmodifiableresources = Collections.unmodifiableCollection(resources);
    for (int i = 0; i < min; i++) {
      V res = listener.createNewResource(null, null);
      created.incrementAndGet();
      resources.add(res);
    }
  }

  public V getResource(K key, final long maxWaitMillis, Object... additionalArgs)
      throws OAcquireTimeoutException {
    // First, get permission to take or create a resource
    try {
      if (!sem.tryAcquire(maxWaitMillis, TimeUnit.MILLISECONDS))
        throw new OAcquireTimeoutException(
            "No more resources available in pool (max="
                + maxResources
                + "). Requested resource: "
                + key);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw OException.wrapException(
          new OInterruptedException("Acquiring of resources was interrupted"), e);
    }

    V res;
    do {
      // POP A RESOURCE
      res = resources.poll();
      if (res != null) {
        // TRY TO REUSE IT
        if (listener.reuseResource(key, additionalArgs, res)) {
          // OK: REUSE IT
          break;
        } else res = null;

        // UNABLE TO REUSE IT: THE RESOURE WILL BE DISCARDED AND TRY WITH THE NEXT ONE, IF ANY
      }
    } while (!resources.isEmpty());

    // NO AVAILABLE RESOURCES: CREATE A NEW ONE
    try {
      if (res == null) {
        res = listener.createNewResource(key, additionalArgs);
        created.incrementAndGet();
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(
                  this,
                  "pool:'%s' created new resource '%s', new resource count '%d'",
                  this,
                  res,
                  created.get());
      }
      resourcesOut.add(res);
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(
                this,
                "pool:'%s' acquired resource '%s' available %d out %d ",
                this,
                res,
                sem.availablePermits(),
                resourcesOut.size());
      return res;
    } catch (RuntimeException e) {
      sem.release();
      // PROPAGATE IT
      throw e;
    } catch (Exception e) {
      sem.release();

      throw OException.wrapException(
          new OLockException("Error on creation of the new resource in the pool"), e);
    }
  }

  public int getMaxResources() {
    return maxResources;
  }

  public int getAvailableResources() {
    return sem.availablePermits();
  }

  public int getInPoolResources() {
    return resources.size();
  }

  public boolean returnResource(final V res) {
    if (resourcesOut.remove(res)) {
      resources.add(res);
      sem.release();
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(
                this,
                "pool:'%s' returned resource '%s' available %d out %d",
                this,
                res,
                sem.availablePermits(),
                resourcesOut.size());
    }
    return true;
  }

  public Collection<V> getResources() {
    return unmodifiableresources;
  }

  public void close() {
    sem.drainPermits();
  }

  public Collection<V> getAllResources() {
    List<V> all = new ArrayList<V>(resources);
    all.addAll(resourcesOut);
    return all;
  }

  public void remove(final V res) {
    if (resourcesOut.remove(res)) {
      this.resources.remove(res);
      sem.release();
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance()
            .debug(
                this,
                "pool:'%s' removed resource '%s' available %d out %d",
                this,
                res,
                sem.availablePermits(),
                resourcesOut.size());
    }
  }

  public int getCreatedInstances() {
    return created.get();
  }

  public int getResourcesOutCount() {
    return resourcesOut.size();
  }
}
