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

package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 21/11/14
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
      final DatabaseDocumentTxPolled db = new DatabaseDocumentTxPolled(url, userName, password);
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

            db = new DatabaseDocumentTxPolled(url, userName, password);
            db.reuse(partition);

            data.acquireCount = 1;
            data.acquiredDatabase = db;

            partition.acquiredConnections.incrementAndGet();
            partition.currentSize.incrementAndGet();

            return db;
          }
        } else {
          db.reuse(partition);
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
        db.forceClose();
      }

    }
  }

  private final class DatabaseDocumentTxPolled extends ODatabaseDocumentTx {
    private PoolPartition partition;

    private DatabaseDocumentTxPolled(String iURL, String userName, String password) {
      super(iURL);

      super.open(userName, password);
    }

    @Override
    public DatabaseDocumentTxPolled open(final String iUserName, final String iUserPassword) {
      throw new UnsupportedOperationException(
          "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
    }

    @Override
    public DatabaseDocumentTxPolled create() {
      throw new UnsupportedOperationException(
          "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a ODatabaseDocumentTx instance if you want to manually open the connection");
    }

    public void reuse(final PoolPartition partition) {
      this.partition = partition;

      getLevel1Cache().invalidate();
      // getMetadata().reload();
      ODatabaseRecordThreadLocal.INSTANCE.set(this);

      try {
        ODatabase current = underlying;
        while (!(current instanceof ODatabaseRecordAbstract) && ((ODatabaseComplex<?>) current).getUnderlying() != null)
          current = ((ODatabaseComplex<?>) current).getUnderlying();
        ((ODatabaseRecordAbstract) current).callOnOpenListeners();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on reusing database '%s' in pool", e, getName());
      }
    }

    public boolean isClosed() {
      return partition == null || super.isClosed();
    }

    @Override
    public void close() {
      final PoolData data = poolData.get();
      if (data.acquireCount == 0)
        return;

      data.acquireCount--;

      if (data.acquireCount > 0)
        return;

      try {
        commit(true);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
      }

      try {
        ODatabase current = underlying;
        while (!(current instanceof ODatabaseRecordAbstract) && ((ODatabaseComplex<?>) current).getUnderlying() != null)
          current = ((ODatabaseComplex<?>) current).getUnderlying();
        ((ODatabaseRecordAbstract) current).callOnCloseListeners();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on releasing database '%s' in pool", e, getName());
      }

      getLevel1Cache().clear();

      ODatabaseRecordThreadLocal.INSTANCE.set(null);

      PoolPartition p = partition;
      partition = null;

      data.acquiredDatabase = null;

      p.queue.offer(this);
      p.acquiredConnections.decrementAndGet();
    }

    public void forceClose() {
      super.close();
    }

    protected void checkOpeness() {
      if (partition == null)
        throw new ODatabaseException(
            "Database instance has been released to the pool. Get another database instance from the pool with the right username and password");

      super.checkOpeness();
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