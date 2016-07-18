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
package com.orientechnologies.orient.core.db;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Factory for {@link OPartitionedDatabasePool} pool, which also works as LRU cache with good mutlicore architecture support.
 * <p>
 * In case of remote storage database pool will keep connections to the remote storage till you close pool. So in case of remote
 * storage you should close pool factory at the end of it's usage, it also may be closed on application shutdown but you should not
 * rely on this behaviour.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 06/11/14
 */
public class OPartitionedDatabasePoolFactory extends OOrientListenerAbstract {
  private volatile int     maxPoolSize = 64;
  private          boolean closed      = false;

  private final ConcurrentLinkedHashMap<PoolIdentity, OPartitionedDatabasePool> poolStore;

  private final EvictionListener<PoolIdentity, OPartitionedDatabasePool> evictionListener = new EvictionListener<PoolIdentity, OPartitionedDatabasePool>() {
    @Override
    public void onEviction(PoolIdentity poolIdentity, OPartitionedDatabasePool partitionedDatabasePool) {
      partitionedDatabasePool.close();
    }
  };

  public OPartitionedDatabasePoolFactory() {
    this(100);
  }

  public OPartitionedDatabasePoolFactory(int capacity) {
    poolStore = new ConcurrentLinkedHashMap.Builder<PoolIdentity, OPartitionedDatabasePool>().maximumWeightedCapacity(capacity)
        .listener(evictionListener).build();

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    checkForClose();

    this.maxPoolSize = maxPoolSize;
  }

  public OPartitionedDatabasePool get(String url, String userName, String userPassword) {
    checkForClose();

    final PoolIdentity poolIdentity = new PoolIdentity(url, userName, userPassword);

    OPartitionedDatabasePool pool = poolStore.get(poolIdentity);
    if (pool != null && !pool.isClosed())
      return pool;

    if (pool != null)
      poolStore.remove(poolIdentity, pool);

    while (true) {
      pool = new OPartitionedDatabasePool(url, userName, userPassword, 64, maxPoolSize);

      final OPartitionedDatabasePool oldPool = poolStore.putIfAbsent(poolIdentity, pool);

      if (oldPool != null) {
        if (!oldPool.isClosed()) {
          return oldPool;
        } else {
          poolStore.remove(poolIdentity, oldPool);
        }
      } else {
        return pool;
      }
    }

  }

  public Collection<OPartitionedDatabasePool> getPools() {
    checkForClose();

    return Collections.unmodifiableCollection(poolStore.values());
  }

  public void close() {
    if (closed)
      return;

    closed = true;

    while (!poolStore.isEmpty()) {
      final Iterator<OPartitionedDatabasePool> poolIterator = poolStore.values().iterator();

      while (poolIterator.hasNext()) {
        final OPartitionedDatabasePool pool = poolIterator.next();

        try {
          pool.close();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during pool close", e);
        }

        poolIterator.remove();
      }
    }

    for (OPartitionedDatabasePool pool : poolStore.values())
      pool.close();

    poolStore.clear();
  }

  private void checkForClose() {
    if (closed)
      throw new IllegalStateException("Pool factory is closed");
  }

  @Override
  public void onShutdown() {
    close();
  }

  private static final class PoolIdentity {
    private final String url;
    private final String userName;
    private final String userPassword;

    private PoolIdentity(String url, String userName, String userPassword) {
      this.url = url;
      this.userName = userName;
      this.userPassword = userPassword;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PoolIdentity that = (PoolIdentity) o;

      if (!url.equals(that.url))
        return false;
      if (!userName.equals(that.userName))
        return false;
      if (!userPassword.equals(that.userPassword))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = url.hashCode();
      result = 31 * result + userName.hashCode();
      result = 31 * result + userPassword.hashCode();
      return result;
    }
  }
}
