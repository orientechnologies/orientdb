package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 06/11/14
 */
public class OPartitionedDatabasePool {
  private final String         url;
  private final String         userName;
  private final String         password;

  private final int            maxSize;

  private static final int     HASH_INCREMENT = 0x61c88647;
  private static final int     MIN_POOL_SIZE  = 2;

  private static AtomicInteger nextHashCode   = new AtomicInteger();

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  private final ThreadLocal<PoolData> poolData      = new ThreadLocal<PoolData>() {
                                                      @Override
                                                      protected PoolData initialValue() {
                                                        return new PoolData();
                                                      }
                                                    };

  private final AtomicBoolean         poolBusy      = new AtomicBoolean();
  private final int                   maxPartitions = Runtime.getRuntime().availableProcessors() << 3;

  private volatile PoolPartition[]    partitions;

  private volatile boolean            closed        = false;

  public OPartitionedDatabasePool(String url, String userName, String password) {
    this(url, userName, password, 64);
  }

  public OPartitionedDatabasePool(String url, String userName, String password, int maxSize) {
    this.url = url;
    this.userName = userName;
    this.password = password;
    this.maxSize = maxSize;

    final PoolPartition[] pts = new PoolPartition[2];

    for (int i = 0; i < pts.length; i++) {
      final PoolPartition partition = new PoolPartition();
      pts[i] = partition;

      initQueue(url, partition);
    }

    partitions = pts;

  }

  public String getUrl() {
    return url;
  }

  public String getUserName() {
    return userName;
  }

  private void initQueue(String url, PoolPartition partition) {
    ConcurrentLinkedQueue<DatabaseDocumentTxPolled> queue = partition.queue;

    for (int n = 0; n < MIN_POOL_SIZE; n++) {
      final DatabaseDocumentTxPolled db = new DatabaseDocumentTxPolled(url);
      queue.add(db);
    }

    partition.currentSize.addAndGet(MIN_POOL_SIZE);
  }

  public int getMaxSize() {
    return maxSize;
  }

  public int getAvailableConnections() {
    checkForClose();

    int result = 0;

    for (PoolPartition partition : partitions) {
      if (partition != null) {
        result += partition.currentSize.get() - partition.acquiredConnections.get();
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

    if (result < 0)
      return 0;

    return result;
  }

  public ODatabaseDocumentTx acquire() {
    checkForClose();

    final PoolData data = poolData.get();
    if (data.acquireCount > 0) {
      data.acquireCount++;

      assert data.acquiredDatabase != null;

      return data.acquiredDatabase;
    }

    while (true) {
      final PoolPartition[] pts = partitions;

      final int index = (pts.length - 1) & data.hashCode;

      PoolPartition partition = pts[index];
      if (partition == null) {
        if (!poolBusy.get() && poolBusy.compareAndSet(false, true)) {
          if (pts == partitions) {
            partition = pts[index];

            if (partition == null) {
              partition = new PoolPartition();
              initQueue(url, partition);
              pts[index] = partition;
            }
          }

          poolBusy.set(false);
        }

        continue;
      } else {
        DatabaseDocumentTxPolled db = partition.queue.poll();
        if (db == null) {
          if (pts.length < maxPartitions) {
            if (poolBusy.compareAndSet(false, true)) {
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

            db = new DatabaseDocumentTxPolled(url);
            db.open(userName, password);
            db.partition = partition;

            data.acquireCount = 1;
            data.acquiredDatabase = db;

            partition.acquiredConnections.incrementAndGet();
            partition.currentSize.incrementAndGet();

            return db;
          }
        } else {
          db.open(userName, password);
          db.partition = partition;
          partition.acquiredConnections.incrementAndGet();

          data.acquireCount = 1;
          data.acquiredDatabase = db;

          return db;
        }
      }
    }
  }

  private void checkForClose() {
    if (closed)
      throw new IllegalStateException("Pool is closed");
  }

  public void close() {
    if (closed)
      return;

    closed = true;

    for (PoolPartition partition : partitions) {
      if (partition == null)
        continue;

      final Queue<DatabaseDocumentTxPolled> queue = partition.queue;

      while (!queue.isEmpty()) {
        DatabaseDocumentTxPolled db = queue.poll();
        OStorage storage = db.getStorage();
        storage.close();
      }

    }
  }

  private final class DatabaseDocumentTxPolled extends ODatabaseDocumentTx {
    private PoolPartition partition;

    private DatabaseDocumentTxPolled(String iURL) {
      super(iURL, true);
    }

    @Override
    public void close() {
      final PoolData data = poolData.get();
      if (data.acquireCount == 0)
        return;

      data.acquireCount--;

      if (data.acquireCount > 0)
        return;

      PoolPartition p = partition;
      partition = null;

      super.close();
      data.acquiredDatabase = null;

      p.queue.offer(this);
      p.acquiredConnections.decrementAndGet();
    }
  }

  private static final class PoolData {
    private final int hashCode;

    private PoolData() {
      hashCode = nextHashCode();
    }

    private int                      acquireCount;
    private DatabaseDocumentTxPolled acquiredDatabase;
  }

  private static final class PoolPartition {
    private final AtomicInteger                                   currentSize         = new AtomicInteger();
    private final AtomicInteger                                   acquiredConnections = new AtomicInteger();
    private final ConcurrentLinkedQueue<DatabaseDocumentTxPolled> queue               = new ConcurrentLinkedQueue<DatabaseDocumentTxPolled>();
  }
}
