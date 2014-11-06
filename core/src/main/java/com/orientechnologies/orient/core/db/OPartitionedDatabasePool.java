package com.orientechnologies.orient.core.db;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 06/11/14
 */
public class OPartitionedDatabasePool {
  private final String         url;
  private final String         userName;
  private final String         password;
  private final int            minSize;
  private final int            maxSize;

  private final AtomicInteger  currentSize         = new AtomicInteger();
  private final AtomicInteger  acquiredConnections = new AtomicInteger();

  private static final int     HASH_INCREMENT      = 0x61c88647;

  private static AtomicInteger nextHashCode        = new AtomicInteger();

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  private final ThreadLocal<Integer>                                             threadHashCode = new ThreadLocal<Integer>() {
                                                                                                  @Override
                                                                                                  protected Integer initialValue() {
                                                                                                    return nextHashCode();
                                                                                                  }
                                                                                                };

  private final AtomicBoolean                                                    poolBusy       = new AtomicBoolean();
  private final int                                                              maxPartitions  = Runtime.getRuntime()
                                                                                                    .availableProcessors() << 2;

  private volatile AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>[] partitions;

  public OPartitionedDatabasePool(String url, String userName, String password) {
    this(url, userName, password, 1, 64);
  }

  public OPartitionedDatabasePool(String url, String userName, String password, int minSize, int maxSize) {
    this.url = url;
    this.userName = userName;
    this.password = password;
    this.minSize = minSize;
    this.maxSize = maxSize;

    final AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>[] pts = new AtomicReference[2];
    for (int i = 0; i < pts.length; i++) {
      final ConcurrentLinkedQueue<ODatabaseDocumentTx> queue = new ConcurrentLinkedQueue<ODatabaseDocumentTx>();
      pts[i] = new AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>(queue);

      initQueue(url, minSize, queue);
    }

    partitions = pts;

  }

  private void initQueue(String url, int minSize, ConcurrentLinkedQueue<ODatabaseDocumentTx> queue) {
    for (int n = 0; n < minSize; n++) {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
      queue.add(db);
    }

    currentSize.addAndGet(minSize);
  }

  public ODatabaseDocument acquire() {
    final AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>[] pts = partitions;

    acquiredConnections.incrementAndGet();
    try {
      while (true) {
        final int index = (pts.length - 1) & threadHashCode.get();

        ConcurrentLinkedQueue<ODatabaseDocumentTx> queue = pts[index].get();
        if (queue == null) {
          if (poolBusy.compareAndSet(false, true)) {
            if (pts == partitions) {
              final AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>> queueRef = pts[index];

              if (queueRef.get() == null) {
                queue = new ConcurrentLinkedQueue<ODatabaseDocumentTx>();
                initQueue(url, minSize, queue);
                queueRef.set(queue);
              }
            }

            poolBusy.set(false);
          }

          continue;
        } else {
          ODatabaseDocumentTx db = queue.poll();
          if (db == null) {
            if (pts.length < maxPartitions) {
              if (poolBusy.compareAndSet(false, true)) {
                if (pts == partitions) {
                  final AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>[] newPartitions = new AtomicReference[partitions.length << 1];
                  System.arraycopy(partitions, 0, newPartitions, 0, partitions.length);

                  for (int i = partitions.length - 1; i < newPartitions.length; i++)
                    newPartitions[i] = new AtomicReference<ConcurrentLinkedQueue<ODatabaseDocumentTx>>();

                  partitions = newPartitions;
                }

                poolBusy.set(false);
              }

              continue;
            } else {
              if (currentSize.get() >= maxSize)
                throw new IllegalStateException("You have reached maximum pool size");

              db = new ODatabaseDocumentTx(url);
              db.open(userName, password);

              currentSize.incrementAndGet();

              return wrapDb(db, queue);
            }
          } else {
            db.open(userName, password);

            return wrapDb(db, queue);
          }
        }
      }
    } catch (RuntimeException e) {
      acquiredConnections.decrementAndGet();
      throw e;
    }
  }

  public void release(ODatabaseDocument db) {
    if (!(db instanceof DBWrapper))
      throw new IllegalArgumentException("DB was not taken from the pool");

    DBWrapper dbWrapper = (DBWrapper) db;

    db.close();
    ConcurrentLinkedQueue<ODatabaseDocumentTx> queue = dbWrapper.queue;

    queue.offer(dbWrapper.underlying);

    acquiredConnections.decrementAndGet();
  }

  private ODatabaseDocument wrapDb(ODatabaseDocumentTx db, ConcurrentLinkedQueue<ODatabaseDocumentTx> queue) {
    return new DBWrapper(db, queue);
  }

  private static final class DBWrapper extends ODatabaseRecordWrapperAbstract<ODatabaseDocumentTx> {
    private final ConcurrentLinkedQueue<ODatabaseDocumentTx> queue;

    private DBWrapper(ODatabaseDocumentTx database, ConcurrentLinkedQueue<ODatabaseDocumentTx> queue) {
      super(database);
      this.queue = queue;
    }
  }
}
