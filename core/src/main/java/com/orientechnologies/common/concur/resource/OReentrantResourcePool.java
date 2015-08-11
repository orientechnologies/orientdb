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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reentrant implementation of Resource Pool. It manages multiple resource acquisition on thread local map. If you're looking for a
 * Reentrant implementation look at #OReentrantResourcePool.
 * 
 * @author Andrey Lomakin (a.lomakin--at--orientechnologies.com)
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see OResourcePool
 */
public class OReentrantResourcePool<K, V> extends OResourcePool<K, V> implements OOrientStartupListener, OOrientShutdownListener {
  private volatile ThreadLocal<Map<K, ResourceHolder<V>>> activeResources = new ThreadLocal<Map<K, ResourceHolder<V>>>();

  private static final class ResourceHolder<V> {
    private final V resource;
    private int     counter = 1;

    private ResourceHolder(V resource) {
      this.resource = resource;
    }
  }

  public OReentrantResourcePool(final int maxResources, final OResourcePoolListener<K, V> listener) {
    super(maxResources, listener);

    Orient.instance().registerWeakOrientShutdownListener(this);
    Orient.instance().registerWeakOrientStartupListener(this);
  }

  @Override
  public void onShutdown() {
    activeResources = null;
  }

  @Override
  public void onStartup() {
    if (activeResources == null)
      activeResources = new ThreadLocal<Map<K, ResourceHolder<V>>>();
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
    try {
      final V res = super.getResource(key, maxWaitMillis, additionalArgs);
      resourceHolderMap.put(key, new ResourceHolder<V>(res));
      return res;

    } catch (RuntimeException e) {
      resourceHolderMap.remove(key);

      // PROPAGATE IT
      throw e;
    }
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

    return super.returnResource(res);
  }

  public int getConnectionsInCurrentThread(final K key) {
    final Map<K, ResourceHolder<V>> resourceHolderMap = activeResources.get();
    if (resourceHolderMap == null)
      return 0;

    final ResourceHolder<V> holder = resourceHolderMap.get(key);
    if (holder == null)
      return 0;

    return holder.counter;
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
}
