package com.orientechnologies.common.concur.resource;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class OResourcePoolFactory<K, T> extends OOrientListenerAbstract {
  private volatile int maxPartitions = Runtime.getRuntime().availableProcessors() << 3;
  private volatile int maxPoolSize = 64;
  private boolean closed = false;

  private final ConcurrentLinkedHashMap<K, OResourcePool<K, T>> poolStore;
  private final ObjectFactoryFactory<K, T> objectFactoryFactory;

  private final EvictionListener<K, OResourcePool<K, T>> evictionListener =
      new EvictionListener<K, OResourcePool<K, T>>() {
        @Override
        public void onEviction(K key, OResourcePool<K, T> partitionedObjectPool) {
          partitionedObjectPool.close();
        }
      };

  public OResourcePoolFactory(final ObjectFactoryFactory<K, T> objectFactoryFactory) {
    this(objectFactoryFactory, 100);
  }

  public OResourcePoolFactory(
      final ObjectFactoryFactory<K, T> objectFactoryFactory, final int capacity) {
    this.objectFactoryFactory = objectFactoryFactory;
    poolStore =
        new ConcurrentLinkedHashMap.Builder<K, OResourcePool<K, T>>()
            .maximumWeightedCapacity(capacity)
            .listener(evictionListener)
            .build();

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(final int maxPoolSize) {
    checkForClose();

    this.maxPoolSize = maxPoolSize;
  }

  public OResourcePool<K, T> get(final K key) {
    checkForClose();

    OResourcePool<K, T> pool = poolStore.get(key);
    if (pool != null) return pool;

    pool = new OResourcePool<K, T>(maxPoolSize, objectFactoryFactory.create(key));

    final OResourcePool<K, T> oldPool = poolStore.putIfAbsent(key, pool);
    if (oldPool != null) {
      pool.close();
      return oldPool;
    }

    return pool;
  }

  public int getMaxPartitions() {
    return maxPartitions;
  }

  public void setMaxPartitions(final int maxPartitions) {
    this.maxPartitions = maxPartitions;
  }

  public Collection<OResourcePool<K, T>> getPools() {
    checkForClose();

    return Collections.unmodifiableCollection(poolStore.values());
  }

  public void close() {
    if (closed) return;

    closed = true;

    while (!poolStore.isEmpty()) {
      final Iterator<OResourcePool<K, T>> poolIterator = poolStore.values().iterator();

      while (poolIterator.hasNext()) {
        final OResourcePool<K, T> pool = poolIterator.next();

        try {
          pool.close();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during pool close", e);
        }

        poolIterator.remove();
      }
    }

    for (OResourcePool<K, T> pool : poolStore.values()) pool.close();

    poolStore.clear();
  }

  @Override
  public void onShutdown() {
    close();
  }

  private void checkForClose() {
    if (closed) throw new IllegalStateException("Pool factory is closed");
  }

  public interface ObjectFactoryFactory<K, T> {
    OResourcePoolListener<K, T> create(K key);
  }
}
