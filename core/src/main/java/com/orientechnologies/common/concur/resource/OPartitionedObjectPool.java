package com.orientechnologies.common.concur.resource;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is internal API, do not use it.
 *
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 15/12/14
 */
public class OPartitionedObjectPool<T> extends OOrientListenerAbstract {
  private static final int           HASH_INCREMENT = 0x61c88647;
  private static final int           MIN_POOL_SIZE  = 2;
  private static final AtomicInteger nextHashCode   = new AtomicInteger();

  private final int                  maxPartitions  = Runtime.getRuntime().availableProcessors() << 3;
  private final ObjectFactory<T>     factory;
  private final int                  maxSize;
  private final ThreadLocal<Integer> threadHashCode = new ThreadLocal<Integer>() {
                                                      @Override
                                                      protected Integer initialValue() {
                                                        return nextHashCode();
                                                      }
                                                    };

  private final AtomicBoolean        poolBusy       = new AtomicBoolean();
  private volatile PoolPartition[]   partitions;
  private volatile boolean           closed         = false;

  public OPartitionedObjectPool(ObjectFactory factory, int maxSize) {
    this.factory = factory;
    this.maxSize = maxSize;

    final PoolPartition[] pts = new PoolPartition[2];

    for (int i = 0; i < pts.length; i++) {
      final PoolPartition partition = new PoolPartition();
      pts[i] = partition;

      initQueue(partition);
    }

    partitions = pts;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public PoolEntry<T> acquire() {
    checkForClose();

    final int th = threadHashCode.get();
    while (true) {
      final PoolPartition[] pts = partitions;

      final int index = (pts.length - 1) & th;

      PoolPartition<T> partition = pts[index];
      if (partition == null) {
        if (!poolBusy.get() && poolBusy.compareAndSet(false, true)) {
          if (pts == partitions) {
            partition = pts[index];

            if (partition == null) {
              partition = new PoolPartition<T>();
              initQueue(partition);
              pts[index] = partition;
            }
          }

          poolBusy.set(false);
        }

        continue;
      } else {
        T object = partition.queue.poll();
        if (object == null) {
          if (pts.length < maxPartitions) {
            if (!poolBusy.get() && poolBusy.compareAndSet(false, true)) {
              if (pts == partitions) {
                final PoolPartition[] newPartitions = new PoolPartition[partitions.length << 1];
                System.arraycopy(partitions, 0, newPartitions, 0, partitions.length);

                partitions = newPartitions;
              }

              poolBusy.set(false);
            }

            continue;
          } else {
            if (partition.currentSize.get() >= maxSize)
              throw new IllegalStateException("You have reached maximum pool size for given partition");

            object = factory.create();

            partition.acquiredObjects.incrementAndGet();
            partition.currentSize.incrementAndGet();

            return new PoolEntry<T>(partition, object);
          }
        } else {
          if (!factory.isValid(object)) {
            factory.close(object);
            partition.currentSize.decrementAndGet();
            continue;
          }

          factory.init(object);
          partition.acquiredObjects.incrementAndGet();

          return new PoolEntry<T>(partition, object);
        }
      }
    }
  }

  public void release(PoolEntry<T> entry) {
    final PoolPartition<T> partition = entry.partition;
    partition.queue.offer(entry.object);
    partition.acquiredObjects.decrementAndGet();
  }

  public int getMaxSize() {
    return maxSize;
  }

  public void close() {
    if (closed)
      return;

    closed = true;

    for (PoolPartition partition : partitions) {
      if (partition == null)
        continue;

      final Queue<T> queue = partition.queue;

      while (!queue.isEmpty()) {
        final T object = queue.poll();
        factory.close(object);
      }

    }
  }

  @Override
  public void onShutdown() {
    close();
  }

  public int getAvailableObjects() {
    checkForClose();

    int result = 0;

    for (PoolPartition partition : partitions) {
      if (partition != null) {
        result += partition.currentSize.get() - partition.acquiredObjects.get();
      }
    }

    if (result < 0)
      return 0;

    return result;
  }

  public int getCreatedInstances() {
    checkForClose();

    int result = 0;

    for (PoolPartition partition : partitions) {
      if (partition != null) {
        result += partition.currentSize.get();
      }
    }

    return result;
  }

  private void initQueue(PoolPartition<T> partition) {
    ConcurrentLinkedQueue<T> queue = partition.queue;

    for (int n = 0; n < MIN_POOL_SIZE; n++) {
      final T object = factory.create();
      queue.add(object);
    }

    partition.currentSize.addAndGet(MIN_POOL_SIZE);
  }

  private void checkForClose() {
    if (closed)
      throw new IllegalStateException("Pool is closed");
  }

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  private static final class PoolPartition<T> {
    private final AtomicInteger            currentSize     = new AtomicInteger();
    private final AtomicInteger            acquiredObjects = new AtomicInteger();
    private final ConcurrentLinkedQueue<T> queue           = new ConcurrentLinkedQueue<T>();
  }

  public interface ObjectFactory<T> {
    T create();

    void init(T object);

    void close(T object);

    boolean isValid(T object);
  }

  public static final class PoolEntry<T> {
    private final PoolPartition<T> partition;
    public final T                 object;

    public PoolEntry(PoolPartition<T> partition, T object) {
      this.partition = partition;
      this.object = object;
    }
  }
}