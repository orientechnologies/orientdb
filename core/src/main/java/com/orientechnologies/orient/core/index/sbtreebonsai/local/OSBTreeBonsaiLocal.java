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

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.exception.OSBTreeBonsaiLocalException;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Tree-based dictionary algorithm. Similar to {@link OSBTree} but uses subpages of disk cache that is more efficient for small data
 * structures.
 * <p>
 * Oriented for usage of several instances inside of one file.
 * <p>
 * Creation of several instances that represent the same collection is not allowed.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @author Artem Orobets
 * @see OSBTree
 * @since 1.6.0
 */
public class OSBTreeBonsaiLocal<K, V> extends ODurableComponent implements OSBTreeBonsai<K, V> {
  private static final OLockManager<Long> FILE_LOCK_MANAGER = new OPartitionedLockManager<Long>();

  private static final int                  PAGE_SIZE             =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  private final        float                freeSpaceReuseTrigger = OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER
      .getValueAsFloat();
  private static final OBonsaiBucketPointer SYS_BUCKET            = new OBonsaiBucketPointer(0, 0);

  private OBonsaiBucketPointer rootBucketPointer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private volatile Long fileId = -1l;

  private OBinarySerializer<K> keySerializer;
  private OBinarySerializer<V> valueSerializer;

  public OSBTreeBonsaiLocal(String name, String dataFileExtension, OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
  }

  public void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(false);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during sbtree creation", this), e);
      }

      Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(-1l);
      try {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        if (isFileExists(atomicOperation, getFullName()))
          this.fileId = openFile(atomicOperation, getFullName());
        else
          this.fileId = addFile(atomicOperation, getFullName());

        initAfterCreate(atomicOperation);

        endAtomicOperation(false, null);
      } catch (IOException e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error creation of sbtree with name" + getName(), this), e);
      } catch (Exception e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error creation of sbtree with name" + getName(), this), e);
      } finally {
        lock.unlock();
      }
    } finally {
      completeOperation();
    }
  }

  private void initAfterCreate(OAtomicOperation atomicOperation) throws IOException {
    initSysBucket(atomicOperation);

    final AllocationResult allocationResult = allocateBucketForWrite(atomicOperation);
    OCacheEntry rootCacheEntry = allocationResult.getCacheEntry();
    this.rootBucketPointer = allocationResult.getPointer();

    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, this.rootBucketPointer.getPageOffset(),
          true, keySerializer, valueSerializer, this);
      rootBucket.setTreeSize(0);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }
  }

  @Override
  public long getFileId() {
    final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
    try {
      return fileId;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
    try {
      return rootBucketPointer;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public OBonsaiCollectionPointer getCollectionPointer() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        return new OBonsaiCollectionPointer(fileId, rootBucketPointer);
      } finally {
        lock.unlock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public V get(K key) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0)
            return null;

          OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

          OCacheEntry keyBucketCacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
          try {
            OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
                keySerializer, valueSerializer, this);
            return keyBucket.getEntry(bucketSearchResult.itemIndex).value;
          } finally {
            releasePageFromRead(atomicOperation, keyBucketCacheEntry);
          }
        } finally {
          lock.unlock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during retrieving  of sbtree with name " + getName(), this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(1);
      completeOperation();
    }
  }

  @Override
  public boolean put(K key, V value) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryUpdateTimer();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during sbtree entrie put", this), e);
      }

      final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

        OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, this);

        final boolean itemFound = bucketSearchResult.itemIndex >= 0;
        boolean result = true;
        if (itemFound) {
          final int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, value);
          assert updateResult == 0 || updateResult == 1;

          result = updateResult != 0;
        } else {
          int insertionIndex = -bucketSearchResult.itemIndex - 1;

          while (!keyBucket.addEntry(insertionIndex,
              new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value), true)) {
            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

            bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);
            bucketPointer = bucketSearchResult.getLastPathItem();

            insertionIndex = bucketSearchResult.itemIndex;

            keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem().getPageIndex(),
                false);

            keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(), keySerializer,
                valueSerializer, this);
          }
        }

        releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

        if (!itemFound)
          setSize(size() + 1, atomicOperation);

        endAtomicOperation(false, null);
        return result;
      } catch (IOException e) {
        rollback(e);
        throw OException.wrapException(
            new OSBTreeBonsaiLocalException("Error during index update with key " + key + " and value " + value, this), e);
      } finally {
        lock.unlock();
      }

    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryUpdateTimer();
      completeOperation();
    }
  }

  private void rollback(Exception e) {
    try {
      endAtomicOperation(true, e);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  public void close(boolean flush) {
    startOperation();
    try {
      Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        readCache.closeFile(fileId, flush, writeCache);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during close of index " + getName(), this), e);
      } finally {
        lock.unlock();
      }
    } finally {
      completeOperation();
    }
  }

  public void close() {
    close(true);
  }

  /**
   * Removes all entries from bonsai tree. Put all but the root page to free list for further reuse.
   */
  @Override
  public void clear() {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during sbtree entrie clear", this), e);
      }

      final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();

        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false);
        try {
          OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(),
              keySerializer, valueSerializer, this);

          addChildrenToQueue(subTreesToDelete, rootBucket);

          rootBucket.shrink(0);
          rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(), true, keySerializer,
              valueSerializer, this);

          rootBucket.setTreeSize(0);
        } finally {
          releasePageFromWrite(atomicOperation, cacheEntry);
        }

        recycleSubTrees(subTreesToDelete, atomicOperation);

        endAtomicOperation(false, null);
      } catch (IOException e) {
        rollback(e);

        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during clear of sbtree with name " + getName(), this), e);
      } catch (RuntimeException e) {
        rollback(e);

        throw e;
      } finally {
        lock.unlock();
      }
    } finally {
      completeOperation();
    }
  }

  private void addChildrenToQueue(Queue<OBonsaiBucketPointer> subTreesToDelete, OSBTreeBonsaiBucket<K, V> rootBucket) {
    if (!rootBucket.isLeaf()) {
      final int size = rootBucket.size();
      if (size > 0)
        subTreesToDelete.add(rootBucket.getEntry(0).leftChild);

      for (int i = 0; i < size; i++) {
        final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = rootBucket.getEntry(i);
        subTreesToDelete.add(entry.rightChild);
      }
    }
  }

  private void recycleSubTrees(Queue<OBonsaiBucketPointer> subTreesToDelete, OAtomicOperation atomicOperation) throws IOException {
    OBonsaiBucketPointer head = OBonsaiBucketPointer.NULL;
    OBonsaiBucketPointer tail = subTreesToDelete.peek();

    int bucketCount = 0;
    while (!subTreesToDelete.isEmpty()) {
      final OBonsaiBucketPointer bucketPointer = subTreesToDelete.poll();
      OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
      try {
        final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, this);

        addChildrenToQueue(subTreesToDelete, bucket);

        bucket.setFreeListPointer(head);
        bucket.setDelted(true);
        head = bucketPointer;
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
      bucketCount++;
    }

    if (head.isValid()) {
      final OCacheEntry sysCacheEntry = loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false);
      try {
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);

        attachFreeListHead(tail, sysBucket.getFreeListHead(), atomicOperation);
        sysBucket.setFreeListHead(head);
        sysBucket.setFreeListLength(sysBucket.freeListLength() + bucketCount);

      } finally {
        releasePageFromWrite(atomicOperation, sysCacheEntry);
      }
    }
  }

  private void attachFreeListHead(OBonsaiBucketPointer bucketPointer, OBonsaiBucketPointer freeListHead,
      OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, this);

      bucket.setFreeListPointer(freeListHead);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  /**
   * Deletes a whole tree. Puts all its pages to free list for further reusage.
   */
  @Override
  public void delete() {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(false);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during sbtree deletion", this), e);
      }

      final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();
        subTreesToDelete.add(rootBucketPointer);
        recycleSubTrees(subTreesToDelete, atomicOperation);

        endAtomicOperation(false, null);
      } catch (Exception e) {
        rollback(e);

        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during delete of sbtree with name " + getName(), this), e);
      } finally {
        lock.unlock();
      }
    } finally {
      completeOperation();
    }
  }

  public boolean load(OBonsaiBucketPointer rootBucketPointer) {
    OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryLoadTimer();
    try {
      Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        this.rootBucketPointer = rootBucketPointer;

        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        this.fileId = openFile(atomicOperation, getFullName());

        OCacheEntry rootCacheEntry = loadPageForRead(atomicOperation, this.fileId, this.rootBucketPointer.getPageIndex(), false);
        try {
          OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry,
              this.rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
          keySerializer = (OBinarySerializer<K>) storage.getComponentsFactory().binarySerializerFactory
              .getObjectSerializer(rootBucket.getKeySerializerId());
          valueSerializer = (OBinarySerializer<V>) storage.getComponentsFactory().binarySerializerFactory
              .getObjectSerializer(rootBucket.getValueSerializerId());

          return !rootBucket.isDeleted();

        } finally {
          releasePageFromRead(atomicOperation, rootCacheEntry);
        }

      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Exception during loading of sbtree " + fileId, this), e);
      } finally {
        lock.unlock();
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryLoadTimer();
      completeOperation();
    }
  }

  private void setSize(long size, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPageForWrite(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false);

    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
          keySerializer, valueSerializer, this);
      rootBucket.setTreeSize(size);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }
  }

  @Override
  public long size() {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          OCacheEntry rootCacheEntry = loadPageForRead(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false);
          try {
            OSBTreeBonsaiBucket rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
                keySerializer, valueSerializer, this);
            return rootBucket.getTreeSize();
          } finally {
            releasePageFromRead(atomicOperation, rootCacheEntry);
          }
        } finally {
          lock.unlock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during retrieving of size of index " + getName(), this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  @Override
  public V remove(K key) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryDeletionTimer();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException("Error during sbtree entrie removal", this), e);
      }

      Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
      try {
        BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        if (bucketSearchResult.itemIndex < 0) {
          endAtomicOperation(false, null);
          return null;
        }

        OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

        OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
        final V removed;

        try {
          OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, this);

          removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

          keyBucket.remove(bucketSearchResult.itemIndex);
        } finally {
          releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
        }
        setSize(size() - 1, atomicOperation);

        endAtomicOperation(false, null);
        return removed;
      } catch (IOException e) {
        rollback(e);

        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during removing key " + key + " from sbtree " + getName(), this),
                e);
      } catch (RuntimeException e) {
        rollback(e);

        throw e;
      } finally {
        lock.unlock();
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryDeletionTimer();
      completeOperation();
    }
  }

  @Override
  public Collection<V> getValuesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
    startOperation();
    try {
      final List<V> result = new ArrayList<V>();

      loadEntriesMinor(key, inclusive, new RangeResultListener<K, V>() {
        @Override
        public boolean addResult(Map.Entry<K, V> entry) {
          result.add(entry.getValue());
          if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      completeOperation();
    }
  }

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    int readEntries = 0;
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

          OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();
          int index;
          if (bucketSearchResult.itemIndex >= 0) {
            index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            index = -bucketSearchResult.itemIndex - 2;
          }

          boolean firstBucket = true;
          do {
            OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
            try {
              OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                  keySerializer, valueSerializer, this);
              if (!firstBucket)
                index = bucket.size() - 1;

              for (int i = index; i >= 0; i--) {
                if (!listener.addResult(bucket.getEntry(i))) {
                  readEntries++;
                  return;
                }

              }

              bucketPointer = bucket.getLeftSibling();

              firstBucket = false;

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          } while (bucketPointer.getPageIndex() >= 0);
        } finally {
          lock.unlock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(
            new OSBTreeBonsaiLocalException("Error during fetch of minor values for key " + key + " in sbtree " + getName(), this),
            ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(readEntries);
      completeOperation();
    }
  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    startOperation();
    try {
      final List<V> result = new ArrayList<V>();

      loadEntriesMajor(key, inclusive, true, new RangeResultListener<K, V>() {
        @Override
        public boolean addResult(Map.Entry<K, V> entry) {
          result.add(entry.getValue());
          if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      completeOperation();
    }
  }

  /**
   * Load all entries with key greater then specified key.
   *
   * @param key          defines
   * @param inclusive    if true entry with given key is included
   * @param ascSortOrder
   * @param listener
   */
  @Override
  public void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    int readEntries = 0;

    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      if (!ascSortOrder)
        throw new IllegalStateException("Descending sort order is not supported.");

      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

          int index;
          if (bucketSearchResult.itemIndex >= 0) {
            index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            index = -bucketSearchResult.itemIndex - 1;
          }

          do {
            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
            try {
              OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                  keySerializer, valueSerializer, this);
              int bucketSize = bucket.size();
              for (int i = index; i < bucketSize; i++) {
                if (!listener.addResult(bucket.getEntry(i))) {
                  readEntries++;
                  return;
                }

              }

              bucketPointer = bucket.getRightSibling();
              index = 0;
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }

          } while (bucketPointer.getPageIndex() >= 0);
        } finally {
          lock.unlock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(
            new OSBTreeBonsaiLocalException("Error during fetch of major values for key " + key + " in sbtree " + getName(), this),
            ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(readEntries);
      completeOperation();
    }
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      final int maxValuesToFetch) {
    startOperation();
    try {
      final List<V> result = new ArrayList<V>();
      loadEntriesBetween(keyFrom, fromInclusive, keyTo, toInclusive, new RangeResultListener<K, V>() {
        @Override
        public boolean addResult(Map.Entry<K, V> entry) {
          result.add(entry.getValue());
          if (maxValuesToFetch > 0 && result.size() >= maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      completeOperation();
    }
  }

  @Override
  public K firstKey() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

          OBonsaiBucketPointer bucketPointer = rootBucketPointer;

          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false);
          int itemIndex = 0;
          try {
            OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                keySerializer, valueSerializer, this);

            while (true) {
              if (bucket.isLeaf()) {
                if (bucket.isEmpty()) {
                  if (path.isEmpty()) {
                    return null;
                  } else {
                    PagePathItemUnit pagePathItemUnit = path.removeLast();

                    bucketPointer = pagePathItemUnit.bucketPointer;
                    itemIndex = pagePathItemUnit.itemIndex + 1;
                  }
                } else {
                  return bucket.getKey(0);
                }
              } else {
                if (bucket.isEmpty() || itemIndex > bucket.size()) {
                  if (path.isEmpty()) {
                    return null;
                  } else {
                    PagePathItemUnit pagePathItemUnit = path.removeLast();

                    bucketPointer = pagePathItemUnit.bucketPointer;
                    itemIndex = pagePathItemUnit.itemIndex + 1;
                  }
                } else {
                  path.add(new PagePathItemUnit(bucketPointer, itemIndex));

                  if (itemIndex < bucket.size()) {
                    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
                    bucketPointer = entry.leftChild;
                  } else {
                    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex - 1);
                    bucketPointer = entry.rightChild;
                  }

                  itemIndex = 0;
                }
              }

              releasePageFromRead(atomicOperation, cacheEntry);

              cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);

              bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
                  this);
            }
          } finally {
            releasePageFromRead(atomicOperation, cacheEntry);
          }
        } finally {
          lock.unlock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during finding first key in sbtree [" + getName() + "]", this),
                e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(1);
      completeOperation();
    }
  }

  @Override
  public K lastKey() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

          OBonsaiBucketPointer bucketPointer = rootBucketPointer;

          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, this);

          int itemIndex = bucket.size() - 1;
          try {
            while (true) {
              if (bucket.isLeaf()) {
                if (bucket.isEmpty()) {
                  if (path.isEmpty()) {
                    return null;
                  } else {
                    PagePathItemUnit pagePathItemUnit = path.removeLast();

                    bucketPointer = pagePathItemUnit.bucketPointer;
                    itemIndex = pagePathItemUnit.itemIndex - 1;
                  }
                } else {
                  return bucket.getKey(bucket.size() - 1);
                }
              } else {
                if (itemIndex < -1) {
                  if (!path.isEmpty()) {
                    PagePathItemUnit pagePathItemUnit = path.removeLast();

                    bucketPointer = pagePathItemUnit.bucketPointer;
                    itemIndex = pagePathItemUnit.itemIndex - 1;
                  } else
                    return null;
                } else {
                  path.add(new PagePathItemUnit(bucketPointer, itemIndex));

                  if (itemIndex > -1) {
                    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
                    bucketPointer = entry.rightChild;
                  } else {
                    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(0);
                    bucketPointer = entry.leftChild;
                  }

                  itemIndex = OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1;
                }
              }

              releasePageFromRead(atomicOperation, cacheEntry);

              cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);

              bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
                  this);
              if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1)
                itemIndex = bucket.size() - 1;
            }
          } finally {
            releasePageFromRead(atomicOperation, cacheEntry);
          }
        } finally {
          lock.unlock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeBonsaiLocalException("Error during finding first key in sbtree [" + getName() + "]", this),
                e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(1);
      completeOperation();
    }
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      RangeResultListener<K, V> listener) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    int readEntries = 0;

    startOperation();
    if (statistic != null)
      statistic.startRidBagEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom, atomicOperation);

          OBonsaiBucketPointer bucketPointerFrom = bucketSearchResultFrom.getLastPathItem();

          int indexFrom;
          if (bucketSearchResultFrom.itemIndex >= 0) {
            indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
          } else {
            indexFrom = -bucketSearchResultFrom.itemIndex - 1;
          }

          BucketSearchResult bucketSearchResultTo = findBucket(keyTo, atomicOperation);
          OBonsaiBucketPointer bucketPointerTo = bucketSearchResultTo.getLastPathItem();

          int indexTo;
          if (bucketSearchResultTo.itemIndex >= 0) {
            indexTo = toInclusive ? bucketSearchResultTo.itemIndex : bucketSearchResultTo.itemIndex - 1;
          } else {
            indexTo = -bucketSearchResultTo.itemIndex - 2;
          }

          int startIndex = indexFrom;
          int endIndex;
          OBonsaiBucketPointer bucketPointer = bucketPointerFrom;

          resultsLoop:
          while (true) {

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);
            try {
              OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                  keySerializer, valueSerializer, this);
              if (!bucketPointer.equals(bucketPointerTo))
                endIndex = bucket.size() - 1;
              else
                endIndex = indexTo;

              for (int i = startIndex; i <= endIndex; i++) {
                if (!listener.addResult(bucket.getEntry(i))) {
                  readEntries++;
                  break resultsLoop;
                }

              }

              if (bucketPointer.equals(bucketPointerTo))
                break;

              bucketPointer = bucket.getRightSibling();
              if (bucketPointer.getPageIndex() < 0)
                break;

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }

            startIndex = 0;
          }
        } finally {
          lock.unlock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(new OSBTreeBonsaiLocalException(
            "Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + getName(), this), ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopRidBagEntryReadTimer(readEntries);
      completeOperation();
    }
  }

  public void flush() {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
        try {
          writeCache.flush();
        } finally {
          lock.unlock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  private BucketSearchResult splitBucket(List<OBonsaiBucketPointer> path, int keyIndex, K keyToInsert,
      OAtomicOperation atomicOperation) throws IOException {
    final OBonsaiBucketPointer bucketPointer = path.get(path.size() - 1);

    OCacheEntry bucketEntry = loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), false);

    try {
      OSBTreeBonsaiBucket<K, V> bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, this);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> rightEntries = new ArrayList<OSBTreeBonsaiBucket.SBTreeEntry<K, V>>(
          indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (!bucketPointer.equals(rootBucketPointer)) {
        final AllocationResult allocationResult = allocateBucketForWrite(atomicOperation);
        OCacheEntry rightBucketEntry = allocationResult.getCacheEntry();
        final OBonsaiBucketPointer rightBucketPointer = allocationResult.getPointer();

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, this);
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            OBonsaiBucketPointer rightSiblingBucketPointer = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingBucketPointer);
            newRightBucket.setLeftSibling(bucketPointer);

            bucketToSplit.setRightSibling(rightBucketPointer);

            if (rightSiblingBucketPointer.isValid()) {
              final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(atomicOperation, fileId,
                  rightSiblingBucketPointer.getPageIndex(), false);

              OSBTreeBonsaiBucket<K, V> rightSiblingBucket = new OSBTreeBonsaiBucket<K, V>(rightSiblingBucketEntry,
                  rightSiblingBucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
              } finally {
                releasePageFromWrite(atomicOperation, rightSiblingBucketEntry);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentBucketPointer.getPageIndex(), false);

          try {
            OSBTreeBonsaiBucket<K, V> parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry,
                parentBucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
            OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(bucketPointer,
                rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              releasePageFromWrite(atomicOperation, parentCacheEntry);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey,
                  atomicOperation);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentBucketPointer.getPageIndex(), false);

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry, parentBucketPointer.getPageOffset(), keySerializer,
                  valueSerializer, this);
            }

          } finally {
            releasePageFromWrite(atomicOperation, parentCacheEntry);
          }

        } finally {
          releasePageFromWrite(atomicOperation, rightBucketEntry);
        }

        ArrayList<OBonsaiBucketPointer> resultPath = new ArrayList<OBonsaiBucketPointer>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(bucketPointer);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPointer);
        if (splitLeaf) {
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
        }
        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);

      } else {
        long treeSize = bucketToSplit.getTreeSize();

        final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> leftEntries = new ArrayList<OSBTreeBonsaiBucket.SBTreeEntry<K, V>>(
            indexToSplit);

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        final AllocationResult leftAllocationResult = allocateBucketForWrite(atomicOperation);
        OCacheEntry leftBucketEntry = leftAllocationResult.getCacheEntry();
        OBonsaiBucketPointer leftBucketPointer = leftAllocationResult.getPointer();

        final AllocationResult rightAllocationResult = allocateBucketForWrite(atomicOperation);
        OCacheEntry rightBucketEntry = rightAllocationResult.getCacheEntry();
        OBonsaiBucketPointer rightBucketPointer = rightAllocationResult.getPointer();
        try {
          OSBTreeBonsaiBucket<K, V> newLeftBucket = new OSBTreeBonsaiBucket<K, V>(leftBucketEntry,
              leftBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, this);
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketPointer);
        } finally {
          releasePageFromWrite(atomicOperation, leftBucketEntry);
        }

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, this);
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketPointer);
        } finally {
          releasePageFromWrite(atomicOperation, rightBucketEntry);
        }

        bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(), false, keySerializer,
            valueSerializer, this);
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit
            .addEntry(0, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(leftBucketPointer, rightBucketPointer, separationKey, null),
                true);

        ArrayList<OBonsaiBucketPointer> resultPath = new ArrayList<OBonsaiBucketPointer>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(leftBucketPointer);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPointer);

        if (splitLeaf)
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
      }

    } finally {
      releasePageFromWrite(atomicOperation, bucketEntry);
    }
  }

  private BucketSearchResult findBucket(K key, OAtomicOperation atomicOperation) throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();

    while (true) {
      path.add(bucketPointer);
      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex(), false);

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, this);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf())
          return new BucketSearchResult(index, path);

        if (index >= 0)
          entry = keyBucket.getEntry(index);
        else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size())
            entry = keyBucket.getEntry(insertionIndex - 1);
          else
            entry = keyBucket.getEntry(insertionIndex);
        }

      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }

      if (comparator.compare(key, entry.key) >= 0)
        bucketPointer = entry.rightChild;
      else
        bucketPointer = entry.leftChild;
    }
  }

  private void initSysBucket(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry sysCacheEntry = loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false);
    if (sysCacheEntry == null) {
      sysCacheEntry = addPage(atomicOperation, fileId);
      assert sysCacheEntry.getPageIndex() == SYS_BUCKET.getPageIndex();
    }

    try {
      OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
      if (sysBucket.isInitialized()) {
        releasePageFromWrite(atomicOperation, sysCacheEntry);

        sysCacheEntry = loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false);

        sysBucket = new OSysBucket(sysCacheEntry);
        sysBucket.init();
      }
    } finally {
      releasePageFromWrite(atomicOperation, sysCacheEntry);
    }
  }

  private AllocationResult allocateBucketForWrite(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry sysCacheEntry = loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false);
    if (sysCacheEntry == null) {
      sysCacheEntry = addPage(atomicOperation, fileId);
      assert sysCacheEntry.getPageIndex() == SYS_BUCKET.getPageIndex();
    }

    try {
      final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
      if ((1.0 * sysBucket.freeListLength()) / ((1.0 * getFilledUpTo(atomicOperation, fileId)) * PAGE_SIZE
          / OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES) >= freeSpaceReuseTrigger) {
        final AllocationResult allocationResult = reuseBucketFromFreeList(sysBucket, atomicOperation);
        return allocationResult;
      } else {
        final OBonsaiBucketPointer freeSpacePointer = sysBucket.getFreeSpacePointer();
        if (freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES > PAGE_SIZE) {
          final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
          final long pageIndex = cacheEntry.getPageIndex();
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(pageIndex, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));

          return new AllocationResult(new OBonsaiBucketPointer(pageIndex, 0), cacheEntry);
        } else {
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(freeSpacePointer.getPageIndex(),
              freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
          final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, freeSpacePointer.getPageIndex(), false);

          return new AllocationResult(freeSpacePointer, cacheEntry);
        }
      }
    } finally {
      releasePageFromWrite(atomicOperation, sysCacheEntry);
    }
  }

  private AllocationResult reuseBucketFromFreeList(OSysBucket sysBucket, OAtomicOperation atomicOperation) throws IOException {
    final OBonsaiBucketPointer oldFreeListHead = sysBucket.getFreeListHead();
    assert oldFreeListHead.isValid();

    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, oldFreeListHead.getPageIndex(), false);
    final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, oldFreeListHead.getPageOffset(),
        keySerializer, valueSerializer, this);

    sysBucket.setFreeListHead(bucket.getFreeListPointer());
    sysBucket.setFreeListLength(sysBucket.freeListLength() - 1);

    return new AllocationResult(oldFreeListHead, cacheEntry);
  }

  @Override
  public int getRealBagSize(Map<K, Change> changes) {
    startOperation();
    try {
      final Map<K, Change> notAppliedChanges = new HashMap<K, Change>(changes);
      final OModifiableInteger size = new OModifiableInteger(0);
      loadEntriesMajor(firstKey(), true, true, new RangeResultListener<K, V>() {
        @Override
        public boolean addResult(Map.Entry<K, V> entry) {
          final Change change = notAppliedChanges.remove(entry.getKey());
          final int result;

          final Integer treeValue = (Integer) entry.getValue();
          if (change == null)
            result = treeValue;
          else
            result = change.applyTo(treeValue);

          size.increment(result);
          return true;
        }
      });

      for (Change change : notAppliedChanges.values()) {
        size.increment(change.applyTo(0));
      }

      return size.intValue();
    } finally {
      completeOperation();
    }
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
    try {
      return keySerializer;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
    try {
      return valueSerializer;
    } finally {
      lock.unlock();
    }
  }

  private static class AllocationResult {
    private final OBonsaiBucketPointer pointer;
    private final OCacheEntry          cacheEntry;

    private AllocationResult(OBonsaiBucketPointer pointer, OCacheEntry cacheEntry) {
      this.pointer = pointer;
      this.cacheEntry = cacheEntry;
    }

    private OBonsaiBucketPointer getPointer() {
      return pointer;
    }

    private OCacheEntry getCacheEntry() {
      return cacheEntry;
    }
  }

  private static class BucketSearchResult {
    private final int                             itemIndex;
    private final ArrayList<OBonsaiBucketPointer> path;

    private BucketSearchResult(int itemIndex, ArrayList<OBonsaiBucketPointer> path) {
      this.itemIndex = itemIndex;
      this.path = path;
    }

    public OBonsaiBucketPointer getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {
    private final OBonsaiBucketPointer bucketPointer;
    private final int                  itemIndex;

    private PagePathItemUnit(OBonsaiBucketPointer bucketPointer, int itemIndex) {
      this.bucketPointer = bucketPointer;
      this.itemIndex = itemIndex;
    }
  }

  public void debugPrintBucket(PrintStream writer) throws IOException {
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();
    path.add(rootBucketPointer);
    debugPrintBucket(rootBucketPointer, writer, path);
  }

  public void debugPrintBucket(OBonsaiBucketPointer bucketPointer, PrintStream writer, final ArrayList<OBonsaiBucketPointer> path)
      throws IOException {

    final OCacheEntry bucketEntry = loadPageForRead(null, fileId, bucketPointer.getPageIndex(), false);
    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
    try {
      final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, this);
      if (keyBucket.isLeaf()) {
        for (int i = 0; i < path.size(); i++)
          writer.append("\t");
        writer.append(" Leaf backet:" + bucketPointer.getPageIndex() + "|" + bucketPointer.getPageOffset());
        writer
            .append(" left bucket:" + keyBucket.getLeftSibling().getPageIndex() + "|" + keyBucket.getLeftSibling().getPageOffset());
        writer.append(
            " right bucket:" + keyBucket.getRightSibling().getPageIndex() + "|" + keyBucket.getRightSibling().getPageOffset());
        writer.append(" size:" + keyBucket.size());
        writer.append(" content: [");
        for (int index = 0; index < keyBucket.size(); index++) {
          entry = keyBucket.getEntry(index);
          writer.append(entry.getKey() + ",");
        }
        writer.append("\n");
      } else {
        for (int i = 0; i < path.size(); i++)
          writer.append("\t");
        writer.append(" node bucket:" + bucketPointer.getPageIndex() + "|" + bucketPointer.getPageOffset());
        writer
            .append(" left bucket:" + keyBucket.getLeftSibling().getPageIndex() + "|" + keyBucket.getLeftSibling().getPageOffset());
        writer.append(
            " right bucket:" + keyBucket.getRightSibling().getPageIndex() + "|" + keyBucket.getRightSibling().getPageOffset());
        writer.append("\n");
        for (int index = 0; index < keyBucket.size(); index++) {
          entry = keyBucket.getEntry(index);
          for (int i = 0; i < path.size(); i++)
            writer.append("\t");
          writer.append(" entry:" + index + " key: " + entry.getKey() + " left \n");
          OBonsaiBucketPointer next = entry.leftChild;
          path.add(next);
          debugPrintBucket(next, writer, path);
          path.remove(next);
          for (int i = 0; i < path.size(); i++)
            writer.append("\t");
          writer.append(" entry:" + index + " key: " + entry.getKey() + " right \n");
          next = entry.rightChild;
          path.add(next);
          debugPrintBucket(next, writer, path);
          path.remove(next);

        }
      }
    } finally {
      releasePageFromRead(null, bucketEntry);
    }

  }

  @Override
  protected void startOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic
          .startComponentOperation(getFullName(), OSessionStoragePerformanceStatistic.ComponentType.RIDBAG);
    }
  }
}
