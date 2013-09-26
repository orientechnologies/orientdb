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

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.concur.lock.OLockException;

public class OResourcePool<K, V> {
  private final Semaphore             sem;
  private final Queue<V>              resources = new ConcurrentLinkedQueue<V>();
  private final Collection<V>         unmodifiableresources;
  private OResourcePoolListener<K, V> listener;

  public OResourcePool(final int iMaxResources, final OResourcePoolListener<K, V> iListener) {
    if (iMaxResources < 1)
      throw new IllegalArgumentException("iMaxResource must be major than 0");
    listener = iListener;
    sem = new Semaphore(iMaxResources + 1, true);
    unmodifiableresources = Collections.unmodifiableCollection(resources);
  }

  public V getResource(K iKey, final long iMaxWaitMillis, Object... iAdditionalArgs) throws OLockException {

    // First, get permission to take or create a resource
    try {
      if (!sem.tryAcquire(iMaxWaitMillis, TimeUnit.MILLISECONDS))
        throw new OLockException("Not more resources available in pool. Requested resource: " + iKey);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OLockException("Not more resources available in pool. Requested resource: " + iKey, e);
    }

    V res;
    do {
      // POP A RESOURCE
      res = resources.poll();
      if (res != null) {
        // TRY TO REUSE IT
        if (listener.reuseResource(iKey, iAdditionalArgs, res))
          // OK: REUSE IT
          return res;

        // UNABLE TO REUSE IT: THE RESOURE WILL BE DISCARDED AND TRY WITH THE NEXT ONE, IF ANY
      }
    } while (!resources.isEmpty());

    // NO AVAILABLE RESOURCES: CREATE A NEW ONE
    try {
      res = listener.createNewResource(iKey, iAdditionalArgs);
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

  public void returnResource(final V res) {
    resources.add(res);
    sem.release();
  }

  public Collection<V> getResources() {
    return unmodifiableresources;
  }

  public void close() {
    sem.drainPermits();
  }
}
