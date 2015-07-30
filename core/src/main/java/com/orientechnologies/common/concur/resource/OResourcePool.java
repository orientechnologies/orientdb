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
package com.orientechnologies.common.concur.resource;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Generic non reentrant implementation about pool of resources. It pre-allocates a semaphore of maxResources. Resources are lazily
 * created by invoking the listener.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @param <K>
 *          Resource's Key
 * @param <V>
 *          Resource Object
 */
public class OResourcePool<K, V> {
  protected final Semaphore             sem;
  protected final Queue<V>              resources    = new ConcurrentLinkedQueue<V>();
  protected final Queue<V>              resourcesOut = new ConcurrentLinkedQueue<V>();
  protected final Collection<V>         unmodifiableresources;
  protected OResourcePoolListener<K, V> listener;
  protected volatile int                created      = 0;

  public OResourcePool(final int maxResources, final OResourcePoolListener<K, V> listener) {
    if (maxResources < 1)
      throw new IllegalArgumentException("iMaxResource must be major than 0");

    this.listener = listener;
    sem = new Semaphore(maxResources, true);
    unmodifiableresources = Collections.unmodifiableCollection(resources);
  }

  public V getResource(K key, final long maxWaitMillis, Object... additionalArgs) throws OLockException {
    // First, get permission to take or create a resource
    try {
      if (!sem.tryAcquire(maxWaitMillis, TimeUnit.MILLISECONDS))
        throw new OLockException("No more resources available in pool. Requested resource: " + key + " timeout: " + maxWaitMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OInterruptedException(e);
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
        } else
          res = null;

        // UNABLE TO REUSE IT: THE RESOURE WILL BE DISCARDED AND TRY WITH THE NEXT ONE, IF ANY
      }
    } while (!resources.isEmpty());

    // NO AVAILABLE RESOURCES: CREATE A NEW ONE
    try {
      if (res == null) {
        res = listener.createNewResource(key, additionalArgs);
        created++;
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "pool:'%s' created new resource '%s', new resource count '%d'", this, res, created);
      }
      resourcesOut.add(res);
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "pool:'%s' acquired resource '%s' available %d out %d ", this, res,
            sem.availablePermits(), resourcesOut.size());
      return res;
    } catch (RuntimeException e) {
      sem.release();
      // PROPAGATE IT
      throw e;
    } catch (Exception e) {
      sem.release();

      throw new OLockException("Error on creation of the new resource in the pool", e);
    }
  }

  public int getMaxResources() {
    return sem.availablePermits();
  }

  public int getAvailableResources() {
    return resources.size();
  }

  public boolean returnResource(final V res) {
    if (resourcesOut.remove(res)) {
      resources.add(res);
      sem.release();
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "pool:'%s' returned resource '%s' available %d out %d", this, res,
            sem.availablePermits(), resourcesOut.size());
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
        OLogManager.instance().debug(this, "pool:'%s' removed resource '%s' available %d out %d", this, res,
            sem.availablePermits(), resourcesOut.size());
    }
  }

  public int getCreatedInstances() {
    return created;
  }
}
