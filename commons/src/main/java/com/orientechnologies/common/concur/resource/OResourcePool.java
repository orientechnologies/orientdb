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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;

public class OResourcePool<K, V> {
  private final Semaphore                              sem;
  private final Queue<V>                               resources       = new ConcurrentLinkedQueue<V>();
  private final Collection<V>                          unmodifiableresources;
  private OResourcePoolListener<K, V>                  listener;

  private final ThreadLocal<Map<K, ResourceHolder<V>>> activeResources = new ThreadLocal<Map<K, ResourceHolder<V>>>();

  public OResourcePool(final int maxResources, final OResourcePoolListener<K, V> listener) {
    if (maxResources < 1)
      throw new IllegalArgumentException("iMaxResource must be major than 0");

    this.listener = listener;
    sem = new Semaphore(maxResources, true);
    unmodifiableresources = Collections.unmodifiableCollection(resources);
  }

  public V getResource(K key, final long maxWaitMillis, Object... additionalArgs) throws OLockException {
    Map<K, ResourceHolder<V>> resourceHolderMap = activeResources.get();

    if (resourceHolderMap == null) {
      resourceHolderMap = new HashMap<K, ResourceHolder<V>>();
      activeResources.set(resourceHolderMap);
    }

    final ResourceHolder<V> holder = resourceHolderMap.get(key);
    if (holder != null) {
      holder.counter++;
      return holder.resource;
    }

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

      resourceHolderMap.put(key, new ResourceHolder<V>(res));
      return res;
    } catch (RuntimeException e) {
      sem.release();
      resourceHolderMap.remove(key);
      // PROPAGATE IT
      throw e;
    } catch (Exception e) {
      sem.release();
      resourceHolderMap.remove(key);

      throw new OLockException("Error on creation of the new resource in the pool", e);
    }
  }

  public int getAvailableConnections() {
    return sem.availablePermits();
  }

  public boolean returnResource(final V res) {
    final Map<K, ResourceHolder<V>> resourceHolderMap = activeResources.get();
    if (resourceHolderMap != null) {
      K keyToRemove = null;
      for (Map.Entry<K, ResourceHolder<V>> entry : resourceHolderMap.entrySet()) {
        final ResourceHolder<V> holder = entry.getValue();
        if (holder.resource.equals(res)) {
          holder.counter--;
          assert holder.counter >= 0;
          if (holder.counter > 0)
            return false;

          keyToRemove = entry.getKey();
          break;
        }
      }

      resourceHolderMap.remove(keyToRemove);
    }

    resources.add(res);
    sem.release();

    return true;
  }

  public Collection<V> getResources() {
    return unmodifiableresources;
  }

  public int getConnectionsInCurrentThread(K key) {
    final Map<K, ResourceHolder<V>> resourceHolderMap = activeResources.get();
    if (resourceHolderMap == null)
      return 0;

    final ResourceHolder<V> holder = resourceHolderMap.get(key);
    if (holder == null)
      return 0;

    return holder.counter;
  }

  public void close() {
    sem.drainPermits();
  }

  public void remove(final V res) {
    this.resources.remove(res);

    final List<K> activeResourcesToRemove = new ArrayList<K>();
    final Map<K, ResourceHolder<V>> activeResourcesMap = activeResources.get();

    if (activeResourcesMap != null) {
      for (Map.Entry<K, ResourceHolder<V>> entry : activeResourcesMap.entrySet()) {
        final ResourceHolder<V> holder = entry.getValue();
        if (holder.resource.equals(res))
          activeResourcesToRemove.add(entry.getKey());
      }

      for (K resourceKey : activeResourcesToRemove) {
        activeResourcesMap.remove(resourceKey);
        sem.release();
      }
    }
  }

  private static final class ResourceHolder<V> {
    private final V resource;
    private int     counter = 1;

    private ResourceHolder(V resource) {
      this.resource = resource;
    }
  }
}
