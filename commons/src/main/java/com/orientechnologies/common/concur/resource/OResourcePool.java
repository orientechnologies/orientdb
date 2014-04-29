/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.concur.resource;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;

import java.util.Collection;
import java.util.Collections;
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
  protected final Queue<V>              resources = new ConcurrentLinkedQueue<V>();
  protected final Collection<V>         unmodifiableresources;
  protected OResourcePoolListener<K, V> listener;

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
        throw new OLockException("No more resources available in pool. Requested resource: " + key);
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
      if (res == null)
        res = listener.createNewResource(key, additionalArgs);

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
    resources.add(res);
    sem.release();
    return true;
  }

  public Collection<V> getResources() {
    return unmodifiableresources;
  }

  public void close() {
    sem.drainPermits();
  }

  public void remove(final V res) {
    this.resources.remove(res);
    sem.release();
  }
}
