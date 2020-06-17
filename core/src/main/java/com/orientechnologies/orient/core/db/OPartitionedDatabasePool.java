/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Database pool which has good multicore scalability characteristics because of creation of several
 * partitions for each logical thread group which drastically decrease thread contention when we
 * acquire new connection to database.
 *
 * <p>To acquire connection from the pool call {@link #acquire()} method but to release connection
 * you just need to call {@link
 * com.orientechnologies.orient.core.db.document.ODatabaseDocument#close()} method.
 *
 * <p>In case of remote storage database pool will keep connections to the remote storage till you
 * close pool. So in case of remote storage you should close pool at the end of it's usage, it also
 * may be closed on application shutdown but you should not rely on this behaviour.
 *
 * <p>This pool has one noticeable difference from other pools. If you perform several subsequent
 * acquire calls in the same thread the <b>same</b> instance of database will be returned, but
 * amount of calls to close method should match to amount of acquire calls to release database back
 * in the pool. It will allow you to use such feature as transaction propagation when you perform
 * call of one service from another one.
 *
 * <p>Given pool has two parameters now, amount of maximum connections for single partition and
 * total amount of connections which may be hold by pool. When you start to use pool it will
 * automatically split by several partitions, each partition is independent from other which gives
 * us very good multicore scalability. Amount of partitions will be close to amount of cores but it
 * is not mandatory and depends how much application is loaded. Amount of connections which may be
 * hold by single partition is defined by user but we suggest to use default parameters if your
 * application load is not extremely high.
 *
 * <p>If total amount of connections which allowed to be hold by this pool is reached thread will
 * wait till free connection will be available. If total amount of connection is set to value 0 or
 * less it means that there is no connection limit.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 06/11/14
 */
public class OPartitionedDatabasePool extends OOrientListenerAbstract {
  private static final int HASH_INCREMENT = 0x61c88647;
  private static final int MIN_POOL_SIZE = 2;
  private static final AtomicInteger nextHashCode = new AtomicInteger();
  protected final Map<String, Object> properties = new HashMap<String, Object>();
  private final String url;
  private final String userName;
  private final String password;
  private final int maxPartitonSize;
  private final AtomicBoolean poolBusy = new AtomicBoolean();
  private int maxPartitions = Runtime.getRuntime().availableProcessors();
  private final Semaphore connectionsCounter;
  private volatile ThreadLocal<PoolData> poolData = new ThreadPoolData();
  private volatile PoolPartition[] partitions;
  private volatile boolean closed = false;
  private boolean autoCreate = false;

  public OPartitionedDatabasePool(String url, String userName, String password) {
    this(url, userName, password, Runtime.getRuntime().availableProcessors(), -1);
  }

  public OPartitionedDatabasePool(
      String url, String userName, String password, int maxPartitionSize, int maxPoolSize) {
    this.url = url;
    this.userName = userName;
    this.password = password;
    if (maxPoolSize > 0) {
      connectionsCounter = new Semaphore(maxPoolSize);
      this.maxPartitions = 1;
      this.maxPartitonSize = maxPoolSize;
    } else {
      this.maxPartitonSize = maxPartitionSize;
      connectionsCounter = null;
    }

    final PoolPartition[] pts = new PoolPartition[maxPartitions];

    for (int i = 0; i < pts.length; i++) {
      final PoolPartition partition = new PoolPartition();
      pts[i] = partition;

      initQueue(url, partition);
    }

    partitions = pts;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  public String getUrl() {
    return url;
  }

  public String getUserName() {
    return userName;
  }

  public int getMaxPartitonSize() {
    return maxPartitonSize;
  }

  public int getAvailableConnections() {
    checkForClose();

    int result = 0;

    for (PoolPartition partition : partitions) {
      if (partition != null) {
        result += partition.currentSize.get() - partition.acquiredConnections.get();
      }
    }

    if (result < 0) return 0;

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

    if (result < 0) return 0;

    return result;
  }

  public ODatabaseDocumentTx acquire() {
    checkForClose();

    final PoolData data = poolData.get();
    if (data.acquireCount > 0) {
      data.acquireCount++;

      assert data.acquiredDatabase != null;

      final ODatabaseDocumentTx db = data.acquiredDatabase;

      db.activateOnCurrentThread();

      properties.entrySet().forEach(p -> db.setProperty(p.getKey(), p.getValue()));
      return db;
    }

    try {
      if (connectionsCounter != null) connectionsCounter.acquire();
    } catch (InterruptedException ie) {
      throw OException.wrapException(
          new OInterruptedException("Acquiring of new connection was interrupted"), ie);
    }

    boolean acquired = false;
    try {
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
          final DatabaseDocumentTxPooled db = partition.queue.poll();
          if (db == null) {
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
              if (partition.currentSize.get() >= maxPartitonSize)
                throw new IllegalStateException(
                    "You have reached maximum pool size for given partition");

              final DatabaseDocumentTxPooled newDb = new DatabaseDocumentTxPooled(url);
              properties.entrySet().forEach(p -> newDb.setProperty(p.getKey(), p.getValue()));
              openDatabase(newDb);
              newDb.partition = partition;

              data.acquireCount = 1;
              data.acquiredDatabase = newDb;

              partition.acquiredConnections.incrementAndGet();
              partition.currentSize.incrementAndGet();

              acquired = true;
              return newDb;
            }
          } else {
            properties.entrySet().forEach(p -> db.setProperty(p.getKey(), p.getValue()));

            openDatabase(db);
            db.partition = partition;
            partition.acquiredConnections.incrementAndGet();

            data.acquireCount = 1;
            data.acquiredDatabase = db;

            acquired = true;

            return db;
          }
        }
      }
    } finally {
      if (!acquired && connectionsCounter != null) connectionsCounter.release();
    }
  }

  public boolean isAutoCreate() {
    return autoCreate;
  }

  public OPartitionedDatabasePool setAutoCreate(final boolean autoCreate) {
    this.autoCreate = autoCreate;
    return this;
  }

  public boolean isClosed() {
    return closed;
  }

  protected void openDatabase(final DatabaseDocumentTxPooled db) {
    if (autoCreate) {
      if (!db.getURL().startsWith("remote:") && !db.exists()) {
        try {
          db.create();
        } catch (OStorageExistsException ex) {
          OLogManager.instance()
              .debug(
                  this,
                  "Can not create storage " + db.getStorage() + " because it already exists.",
                  ex);
          db.internalOpen();
        }
      } else {
        db.internalOpen();
      }
    } else {
      db.internalOpen();
    }
  }

  @Override
  public void onShutdown() {
    close();
  }

  @Override
  public void onStartup() {
    if (poolData == null) poolData = new ThreadPoolData();
  }

  public void close() {
    if (closed) return;

    closed = true;

    for (PoolPartition partition : partitions) {
      if (partition == null) continue;

      final Queue<DatabaseDocumentTxPooled> queue = partition.queue;

      while (!queue.isEmpty()) {
        DatabaseDocumentTxPooled db = queue.poll();
        if (!db.isClosed()) {
          db.activateOnCurrentThread();
          db.close();
        }
      }
    }

    partitions = null;
    poolData = null;
  }

  private void initQueue(String url, PoolPartition partition) {
    ConcurrentLinkedQueue<DatabaseDocumentTxPooled> queue = partition.queue;

    for (int n = 0; n < MIN_POOL_SIZE; n++) {
      final DatabaseDocumentTxPooled db = new DatabaseDocumentTxPooled(url);
      properties.entrySet().forEach(p -> db.setProperty(p.getKey(), p.getValue()));
      queue.add(db);
    }

    partition.currentSize.addAndGet(MIN_POOL_SIZE);
  }

  private void checkForClose() {
    if (closed) throw new IllegalStateException("Pool is closed");
  }

  /**
   * Sets a property value
   *
   * @param iName Property name
   * @param iValue new value to set
   * @return The previous value if any, otherwise null
   */
  public Object setProperty(final String iName, final Object iValue) {
    if (iValue != null) {
      return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
    } else {
      return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    }
  }

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   */
  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase(Locale.ENGLISH));
  }

  private static final class PoolData {
    private final int hashCode;
    private int acquireCount;
    private DatabaseDocumentTxPooled acquiredDatabase;

    private PoolData() {
      hashCode = nextHashCode();
    }
  }

  private static final class PoolPartition {
    private final AtomicInteger currentSize = new AtomicInteger();
    private final AtomicInteger acquiredConnections = new AtomicInteger();
    private final ConcurrentLinkedQueue<DatabaseDocumentTxPooled> queue =
        new ConcurrentLinkedQueue<DatabaseDocumentTxPooled>();
  }

  private static class ThreadPoolData extends ThreadLocal<PoolData> {
    @Override
    protected PoolData initialValue() {
      return new PoolData();
    }
  }

  private final class DatabaseDocumentTxPooled extends ODatabaseDocumentTx {
    private PoolPartition partition;

    private DatabaseDocumentTxPooled(String iURL) {
      super(iURL, false);
    }

    @Override
    public <DB extends ODatabase> DB open(OToken iToken) {
      throw new ODatabaseException("Impossible to open a database managed by a pool ");
    }

    @Override
    public <DB extends ODatabase> DB open(String iUserName, String iUserPassword) {
      throw new ODatabaseException("Impossible to open a database managed by a pool ");
    }

    /**
     * @return <code>true</code> if database is obtained from the pool and <code>false</code>
     *     otherwise.
     */
    @Override
    public boolean isPooled() {
      return true;
    }

    protected void internalOpen() {
      if (internal == null) {
        super.open(userName, password);
      } else {
        internal.activateOnCurrentThread();
        internal.setStatus(STATUS.OPEN);
      }
      if (getMetadata().getSchema().countClasses() == 0) getMetadata().reload();
    }

    @Override
    public void close() {
      if (poolData != null) {
        final PoolData data = poolData.get();
        if (data.acquireCount == 0) return;

        data.acquireCount--;

        if (data.acquireCount > 0) return;

        PoolPartition p = partition;
        partition = null;

        final OStorage storage = getStorage();
        if (storage == null) return;

        // if connection is lost and storage is closed as result we should not put closed connection
        // back to the pool
        if (!storage.isClosed()) {
          activateOnCurrentThread();
          super.internal.internalClose(true);

          data.acquiredDatabase = null;

          p.queue.offer(this);
        } else {
          // close database instance but be ready that it will throw exception because of storage is
          // closed
          try {
            super.close();
          } catch (Exception e) {
            OLogManager.instance()
                .error(
                    this,
                    "Error during closing of database % when storage %s was already closed",
                    e,
                    getUrl(),
                    storage.getName());
          }

          data.acquiredDatabase = null;

          // we create new connection instead of old one
          final DatabaseDocumentTxPooled db = new DatabaseDocumentTxPooled(url);
          p.queue.offer(db);
        }

        if (connectionsCounter != null) connectionsCounter.release();

        p.acquiredConnections.decrementAndGet();
      } else {
        super.close();
      }
    }
  }
}
