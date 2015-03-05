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

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * Tree-based dictionary algorithm. Similar to {@link OSBTree} but uses subpages of disk cache that is more efficient for small data
 * structures.
 * <p/>
 * Oriented for usage of several instances inside of one file.
 * <p/>
 * Creation of several instances that represent the same collection is not allowed.
 * 
 * @author Andrey Lomakin
 * @author Artem Orobets
 * @see OSBTree
 * @since 1.6.0
 */
public class OSBTreeBonsaiLocal<K, V> extends ODurableComponent implements OSBTreeBonsai<K, V> {
  private static final int                  PAGE_SIZE             = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  private final float                       freeSpaceReuseTrigger = OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER
                                                                      .getValueAsFloat();
  private static final OBonsaiBucketPointer SYS_BUCKET            = new OBonsaiBucketPointer(0, 0);

  private OBonsaiBucketPointer              rootBucketPointer;

  private final Comparator<? super K>       comparator            = ODefaultComparator.INSTANCE;

  private OAbstractPaginatedStorage storage;
  private String                            name;

  private final String                      dataFileExtension;

  private ODiskCache                        diskCache;

  private long                              fileId;

  private OBinarySerializer<K>              keySerializer;
  private OBinarySerializer<V>              valueSerializer;

  private final boolean                     durableInNonTxMode;

  public OSBTreeBonsaiLocal(String dataFileExtension, boolean durableInNonTxMode) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
    this.durableInNonTxMode = durableInNonTxMode;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    create(name, keySerializer, valueSerializer, (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying());
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OAbstractPaginatedStorage storageLocal) {
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      this.fileId = diskCache.openFile(name + dataFileExtension);
      this.name = name;

      initAfterCreate();
    } catch (IOException e) {
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    }
  }

  public void create(long fileId, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    create(fileId, keySerializer, valueSerializer, (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage()
        .getUnderlying());
  }

  public void create(long fileId, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OAbstractPaginatedStorage storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      diskCache.openFile(fileId);
      this.fileId = fileId;
      this.name = resolveTreeName(fileId);

      initAfterCreate();
    } catch (IOException e) {
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void initAfterCreate() {
    initDurableComponent(storage);

    try {
      initSysBucket();

      super.startAtomicOperation();

      final AllocationResult allocationResult = allocateBucket();
      OCacheEntry rootCacheEntry = allocationResult.getCacheEntry();
      this.rootBucketPointer = allocationResult.getPointer();

      rootCacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry,
            this.rootBucketPointer.getPageOffset(), true, keySerializer, valueSerializer, getTrackMode());
        rootBucket.setTreeSize(0);

        super.logPageChanges(rootBucket, fileId, this.rootBucketPointer.getPageIndex(), true);
      } finally {
        rootCacheEntry.releaseExclusiveLock();
        diskCache.release(rootCacheEntry);
      }

      super.endAtomicOperation(false);
    } catch (Throwable e) {
      try {
        super.endAtomicOperation(true);
      } catch (IOException e1) {
        OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
      }
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    }
  }

  private void initDurableComponent(OAbstractPaginatedStorage storageLocal) {
    init(storageLocal);
  }

  public String getName() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getFileId() {
    acquireSharedLock();
    try {
      return fileId;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    acquireSharedLock();
    try {
      return rootBucketPointer;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OBonsaiCollectionPointer getCollectionPointer() {
    acquireSharedLock();
    try {
      return new OBonsaiCollectionPointer(fileId, rootBucketPointer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public V get(K key) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.itemIndex < 0) {
          return null;
      }

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      try {
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        return keyBucket.getEntry(bucketSearchResult.itemIndex).value;
      } finally {
        diskCache.release(keyBucketCacheEntry);
      }

    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving  of sbtree with name " + name, e);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean put(K key, V value) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      lockTillAtomicOperationCompletes();

      BucketSearchResult bucketSearchResult = findBucket(key);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      keyBucketCacheEntry.acquireExclusiveLock();
      OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getTrackMode());

      final boolean itemFound = bucketSearchResult.itemIndex >= 0;
      boolean result = true;
      if (itemFound) {
        final int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, value);

        if (updateResult == 1) {
          logPageChanges(keyBucket, fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false);
          keyBucketCacheEntry.markDirty();
        }

        assert updateResult == 0 || updateResult == 1;

        result = updateResult != 0;
      } else {
        int insertionIndex = -bucketSearchResult.itemIndex - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL,
            OBonsaiBucketPointer.NULL, key, value), true)) {
          keyBucketCacheEntry.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);
          bucketPointer = bucketSearchResult.getLastPathItem();

          insertionIndex = bucketSearchResult.itemIndex;

          keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false);
          keyBucketCacheEntry.acquireExclusiveLock();

          keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketPointer.getPageIndex(), false);
      }

      keyBucketCacheEntry.releaseExclusiveLock();
      diskCache.release(keyBucketCacheEntry);

      if (!itemFound) {
          setSize(size() + 1);
      }

      endAtomicOperation(false);
      return result;
    } catch (Throwable e) {
      rollback();
      throw new OSBTreeException("Error during index update with key " + key + " and value " + value, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void rollback() {
    try {
      endAtomicOperation(true);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  public void close(boolean flush) {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId, flush);
    } catch (IOException e) {
      throw new OSBTreeException("Error during close of index " + name, e);
    } finally {
      releaseExclusiveLock();
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
    acquireExclusiveLock();
    try {
      startAtomicOperation();

      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      cacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getTrackMode());

        addChildrenToQueue(subTreesToDelete, rootBucket);

        rootBucket.shrink(0);
        rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(), true, keySerializer,
            valueSerializer, getTrackMode());

        rootBucket.setTreeSize(0);

        logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), true);
        cacheEntry.markDirty();
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      recycleSubTrees(subTreesToDelete);

      endAtomicOperation(false);
    } catch (Throwable e) {
      rollback();

      throw new OSBTreeException("Error during clear of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void addChildrenToQueue(Queue<OBonsaiBucketPointer> subTreesToDelete, OSBTreeBonsaiBucket<K, V> rootBucket) {
    if (!rootBucket.isLeaf()) {
      final int size = rootBucket.size();
      if (size > 0) {
          subTreesToDelete.add(rootBucket.getEntry(0).leftChild);
      }

      for (int i = 0; i < size; i++) {
        final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = rootBucket.getEntry(i);
        subTreesToDelete.add(entry.rightChild);
      }
    }
  }

  private void recycleSubTrees(Queue<OBonsaiBucketPointer> subTreesToDelete) throws IOException {
    OBonsaiBucketPointer head = OBonsaiBucketPointer.NULL;
    OBonsaiBucketPointer tail = subTreesToDelete.peek();

    int bucketCount = 0;
    while (!subTreesToDelete.isEmpty()) {
      final OBonsaiBucketPointer bucketPointer = subTreesToDelete.poll();
      OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      cacheEntry.acquireExclusiveLock();
      try {
        final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getTrackMode());

        addChildrenToQueue(subTreesToDelete, bucket);

        bucket.setFreeListPointer(head);
        head = bucketPointer;

        logPageChanges(bucket, fileId, bucketPointer.getPageIndex(), false);
        cacheEntry.markDirty();
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }
      bucketCount++;
    }

    if (head.isValid()) {
      final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
      sysCacheEntry.acquireExclusiveLock();
      try {
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getTrackMode());

        attachFreeListHead(tail, sysBucket.getFreeListHead());
        sysBucket.setFreeListHead(head);
        sysBucket.setFreeListLength(sysBucket.freeListLength() + bucketCount);

        logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), false);
      } finally {
        sysCacheEntry.releaseExclusiveLock();
        diskCache.release(sysCacheEntry);
      }
    }
  }

  private void attachFreeListHead(OBonsaiBucketPointer bucketPointer, OBonsaiBucketPointer freeListHead) throws IOException {
    OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
    cacheEntry.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getTrackMode());

      bucket.setFreeListPointer(freeListHead);

      super.logPageChanges(bucket, fileId, bucketPointer.getPageIndex(), false);
      cacheEntry.markDirty();
    } finally {
      cacheEntry.releaseExclusiveLock();
      diskCache.release(cacheEntry);
    }
  }

  /**
   * Deletes a whole tree. Puts all its pages to free list for further reusage.
   */
  @Override
  public void delete() {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      lockTillAtomicOperationCompletes();

      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();
      subTreesToDelete.add(rootBucketPointer);
      recycleSubTrees(subTreesToDelete);

      endAtomicOperation(false);
    } catch (Throwable e) {
      rollback();

      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(long fileId, OBonsaiBucketPointer rootBucketPointer, OAbstractPaginatedStorage storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.rootBucketPointer = rootBucketPointer;

      diskCache = storage.getDiskCache();

      diskCache.openFile(fileId);
      this.fileId = fileId;
      this.name = resolveTreeName(fileId);

      OCacheEntry rootCacheEntry = diskCache.load(this.fileId, this.rootBucketPointer.getPageIndex(), false);

      rootCacheEntry.acquireSharedLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry,
            this.rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            rootBucket.getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            rootBucket.getValueSerializerId());
      } finally {
        rootCacheEntry.releaseSharedLock();
        diskCache.release(rootCacheEntry);
      }

      initDurableComponent(storageLocal);
    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + fileId, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private String resolveTreeName(long fileId) {
    final String fileName = diskCache.fileNameById(fileId);
    return fileName.substring(0, fileName.length() - dataFileExtension.length());
  }

  private void setSize(long size) throws IOException {
    OCacheEntry rootCacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);

    rootCacheEntry.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getTrackMode());
      rootBucket.setTreeSize(size);

      logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), false);
      rootCacheEntry.markDirty();
    } finally {
      rootCacheEntry.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }
  }

  @Override
  public long size() {
    acquireSharedLock();
    try {
      OCacheEntry rootCacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      try {
        OSBTreeBonsaiBucket rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
            keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        return rootBucket.getTreeSize();
      } finally {
        diskCache.release(rootCacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving of size of index " + name);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public V remove(K key) {
    acquireExclusiveLock();
    try {

      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.itemIndex < 0) {
          return null;
      }

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      final V removed;

      keyBucketCacheEntry.acquireExclusiveLock();
      try {
        startAtomicOperation();
        lockTillAtomicOperationCompletes();

        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getTrackMode());

        removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

        keyBucket.remove(bucketSearchResult.itemIndex);

        logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
        keyBucketCacheEntry.markDirty();

      } finally {
        keyBucketCacheEntry.releaseExclusiveLock();
        diskCache.release(keyBucketCacheEntry);
      }
      setSize(size() - 1);
      endAtomicOperation(false);
      return removed;

    } catch (Throwable e) {
      rollback();

      throw new OSBTreeException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected void endAtomicOperation(boolean rollback) throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode) {
        return;
    }

    super.endAtomicOperation(rollback);
  }

  @Override
  protected void startAtomicOperation() throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode) {
        return;
    }

    super.startAtomicOperation();
  }

  @Override
  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode) {
        return;
    }

    super.logPageChanges(localPage, fileId, pageIndex, isNewPage);
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode) {
        return ODurablePage.TrackMode.NONE;
    }

    return super.getTrackMode();
  }

  @Override
  public Collection<V> getValuesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();

    loadEntriesMinor(key, inclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        result.add(entry.getValue());
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch) {
            return false;
        }

        return true;
      }
    });

    return result;
  }

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key);

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();
      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
      } else {
        index = -bucketSearchResult.itemIndex - 2;
      }

      boolean firstBucket = true;
      do {
        OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!firstBucket) {
              index = bucket.size() - 1;
          }

          for (int i = index; i >= 0; i--) {
            if (!listener.addResult(bucket.getEntry(i))) {
                return;
            }
          }

          bucketPointer = bucket.getLeftSibling();

          firstBucket = false;

        } finally {
          diskCache.release(cacheEntry);
        }
      } while (bucketPointer.getPageIndex() >= 0);
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of minor values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();

    loadEntriesMajor(key, inclusive, true, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        result.add(entry.getValue());
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch) {
            return false;
        }

        return true;
      }
    });

    return result;
  }

  /**
   * Load all entries with key greater then specified key.
   * 
   * @param key
   *          defines
   * @param inclusive
   *          if true entry with given key is included
   * @param ascSortOrder
   * @param listener
   */
  @Override
  public void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener) {
    if (!ascSortOrder) {
        throw new IllegalStateException("Descending sort order is not supported.");
    }

    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
      } else {
        index = -bucketSearchResult.itemIndex - 1;
      }

      do {
        final OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          int bucketSize = bucket.size();
          for (int i = index; i < bucketSize; i++) {
            if (!listener.addResult(bucket.getEntry(i))) {
                return;
            }
          }

          bucketPointer = bucket.getRightSibling();
          index = 0;
        } finally {
          diskCache.release(cacheEntry);
        }

      } while (bucketPointer.getPageIndex() >= 0);

    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of major values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();
    loadEntriesBetween(keyFrom, fromInclusive, keyTo, toInclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        result.add(entry.getValue());
        if (maxValuesToFetch > 0 && result.size() >= maxValuesToFetch) {
            return false;
        }

        return true;
      }
    });

    return result;
  }

  @Override
  public K firstKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      int itemIndex = 0;

      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer,
          valueSerializer, ODurablePage.TrackMode.NONE);
      try {
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

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);

          bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
        }
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public K lastKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer,
          valueSerializer, ODurablePage.TrackMode.NONE);

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
              } else {
                  return null;
              }
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

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);

          bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
          if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1) {
              itemIndex = bucket.size() - 1;
          }
        }
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom);

      OBonsaiBucketPointer bucketPointerFrom = bucketSearchResultFrom.getLastPathItem();

      int indexFrom;
      if (bucketSearchResultFrom.itemIndex >= 0) {
        indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
      } else {
        indexFrom = -bucketSearchResultFrom.itemIndex - 1;
      }

      BucketSearchResult bucketSearchResultTo = findBucket(keyTo);
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

      resultsLoop: while (true) {

        final OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!bucketPointer.equals(bucketPointerTo)) {
              endIndex = bucket.size() - 1;
          } else {
              endIndex = indexTo;
          }

          for (int i = startIndex; i <= endIndex; i++) {
            if (!listener.addResult(bucket.getEntry(i))) {
                break resultsLoop;
            }
          }

          if (bucketPointer.equals(bucketPointerTo)) {
              break;
          }

          bucketPointer = bucket.getRightSibling();
          if (bucketPointer.getPageIndex() < 0) {
              break;
          }

        } finally {
          diskCache.release(cacheEntry);
        }

        startIndex = 0;
      }

    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public void flush() {
    acquireSharedLock();
    try {
      try {
        diskCache.flushBuffer();
      } catch (IOException e) {
        throw new OSBTreeException("Error during flush of sbtree [" + name + "] data");
      }
    } finally {
      releaseSharedLock();
    }
  }

  private BucketSearchResult splitBucket(List<OBonsaiBucketPointer> path, int keyIndex, K keyToInsert) throws IOException {
    final OBonsaiBucketPointer bucketPointer = path.get(path.size() - 1);
    OCacheEntry bucketEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
    bucketEntry.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getTrackMode());

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> rightEntries = new ArrayList<OSBTreeBonsaiBucket.SBTreeEntry<K, V>>(
          indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++) {
          rightEntries.add(bucketToSplit.getEntry(i));
      }

      if (!bucketPointer.equals(rootBucketPointer)) {
        final AllocationResult allocationResult = allocateBucket();
        OCacheEntry rightBucketEntry = allocationResult.getCacheEntry();
        final OBonsaiBucketPointer rightBucketPointer = allocationResult.getPointer();
        rightBucketEntry.acquireExclusiveLock();

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            OBonsaiBucketPointer rightSiblingBucketPointer = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingBucketPointer);
            newRightBucket.setLeftSibling(bucketPointer);

            bucketToSplit.setRightSibling(rightBucketPointer);

            if (rightSiblingBucketPointer.isValid()) {
              final OCacheEntry rightSiblingBucketEntry = diskCache.load(fileId, rightSiblingBucketPointer.getPageIndex(), false);

              rightSiblingBucketEntry.acquireExclusiveLock();
              OSBTreeBonsaiBucket<K, V> rightSiblingBucket = new OSBTreeBonsaiBucket<K, V>(rightSiblingBucketEntry,
                  rightSiblingBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
                logPageChanges(rightSiblingBucket, fileId, rightSiblingBucketPointer.getPageIndex(), false);
              } finally {
                rightSiblingBucketEntry.releaseExclusiveLock();
                diskCache.release(rightSiblingBucketEntry);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);

          parentCacheEntry.acquireExclusiveLock();
          try {
            OSBTreeBonsaiBucket<K, V> parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry,
                parentBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
            OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(bucketPointer,
                rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentCacheEntry.releaseExclusiveLock();
              diskCache.release(parentCacheEntry);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);

              parentCacheEntry.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry, parentBucketPointer.getPageOffset(), keySerializer,
                  valueSerializer, getTrackMode());
            }

            logPageChanges(parentBucket, fileId, parentBucketPointer.getPageIndex(), false);
          } finally {
            parentCacheEntry.releaseExclusiveLock();

            diskCache.release(parentCacheEntry);
          }

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), allocationResult.isNewPage());
        } finally {
          rightBucketEntry.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        logPageChanges(bucketToSplit, fileId, bucketPointer.getPageIndex(), false);
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

        for (int i = 0; i < indexToSplit; i++) {
            leftEntries.add(bucketToSplit.getEntry(i));
        }

        final AllocationResult leftAllocationResult = allocateBucket();
        OCacheEntry leftBucketEntry = leftAllocationResult.getCacheEntry();
        OBonsaiBucketPointer leftBucketPointer = leftAllocationResult.getPointer();

        final AllocationResult rightAllocationResult = allocateBucket();
        OCacheEntry rightBucketEntry = rightAllocationResult.getCacheEntry();
        OBonsaiBucketPointer rightBucketPointer = rightAllocationResult.getPointer();
        leftBucketEntry.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newLeftBucket = new OSBTreeBonsaiBucket<K, V>(leftBucketEntry,
              leftBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf) {
              newLeftBucket.setRightSibling(rightBucketPointer);
          }

          logPageChanges(newLeftBucket, fileId, leftBucketEntry.getPageIndex(), leftAllocationResult.isNewPage());
        } finally {
          leftBucketEntry.releaseExclusiveLock();
          diskCache.release(leftBucketEntry);
        }

        rightBucketEntry.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          if (splitLeaf) {
              newRightBucket.setLeftSibling(leftBucketPointer);
          }

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), rightAllocationResult.isNewPage());
          rightBucketEntry.markDirty();
        } finally {
          rightBucketEntry.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(), false, keySerializer,
            valueSerializer, getTrackMode());
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit.addEntry(0, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(leftBucketPointer, rightBucketPointer, separationKey,
            null), true);

        logPageChanges(bucketToSplit, fileId, bucketPointer.getPageIndex(), false);
        ArrayList<OBonsaiBucketPointer> resultPath = new ArrayList<OBonsaiBucketPointer>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(leftBucketPointer);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPointer);

        if (splitLeaf) {
            return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
        }

        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
      }

    } finally {
      bucketEntry.releaseExclusiveLock();
      diskCache.release(bucketEntry);
    }
  }

  private BucketSearchResult findBucket(K key) throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();

    while (true) {
      path.add(bucketPointer);
      final OCacheEntry bucketEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf()) {
            return new BucketSearchResult(index, path);
        }

        if (index >= 0) {
            entry = keyBucket.getEntry(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
              entry = keyBucket.getEntry(insertionIndex - 1);
            } else {
              entry = keyBucket.getEntry(insertionIndex);
          }
        }

      } finally {
        diskCache.release(bucketEntry);
      }

      if (comparator.compare(key, entry.key) >= 0) {
          bucketPointer = entry.rightChild;
      } else {
          bucketPointer = entry.leftChild;
      }
    }
  }

  private void initSysBucket() throws IOException {
    final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
    sysCacheEntry.acquireExclusiveLock();
    try {
      final OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getTrackMode());
      if (sysBucket.isInitialized()) {
        super.startAtomicOperation();

        try {
          sysBucket.init();
          super.logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), true);
          sysCacheEntry.markDirty();

          super.endAtomicOperation(false);
        } catch (Throwable e) {
          super.endAtomicOperation(true);
          throw new OStorageException(null, e);
        }
      }
    } finally {
      sysCacheEntry.releaseExclusiveLock();
      diskCache.release(sysCacheEntry);
    }
  }

  private AllocationResult allocateBucket() throws IOException {
    final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
    sysCacheEntry.acquireExclusiveLock();
    try {
      final OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getTrackMode());
      if ((1.0 * sysBucket.freeListLength())
          / (diskCache.getFilledUpTo(fileId) * PAGE_SIZE / OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES) >= freeSpaceReuseTrigger) {
        final AllocationResult allocationResult = reuseBucketFromFreeList(sysBucket);
        sysCacheEntry.markDirty();
        return allocationResult;
      } else {
        final OBonsaiBucketPointer freeSpacePointer = sysBucket.getFreeSpacePointer();
        if (freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES > PAGE_SIZE) {
          final OCacheEntry cacheEntry = diskCache.allocateNewPage(fileId);
          final long pageIndex = cacheEntry.getPageIndex();
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(pageIndex, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));

          logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), false);
          sysCacheEntry.markDirty();

          return new AllocationResult(new OBonsaiBucketPointer(pageIndex, 0), cacheEntry, true);
        } else {
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(freeSpacePointer.getPageIndex(), freeSpacePointer.getPageOffset()
              + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
          final OCacheEntry cacheEntry = diskCache.load(fileId, freeSpacePointer.getPageIndex(), false);

          logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), false);
          return new AllocationResult(freeSpacePointer, cacheEntry, false);
        }
      }
    } finally {
      sysCacheEntry.releaseExclusiveLock();
      diskCache.release(sysCacheEntry);
    }
  }

  private AllocationResult reuseBucketFromFreeList(OSysBucket sysBucket) throws IOException {
    final OBonsaiBucketPointer oldFreeListHead = sysBucket.getFreeListHead();
    assert oldFreeListHead.isValid();

    OCacheEntry cacheEntry = diskCache.load(fileId, oldFreeListHead.getPageIndex(), false);
    cacheEntry.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, oldFreeListHead.getPageOffset(),
          keySerializer, valueSerializer, getTrackMode());

      sysBucket.setFreeListHead(bucket.getFreeListPointer());
      sysBucket.setFreeListLength(sysBucket.freeListLength() - 1);

      logPageChanges(bucket, fileId, oldFreeListHead.getPageIndex(), false);
      cacheEntry.markDirty();
    } finally {
      cacheEntry.releaseExclusiveLock();
    }
    return new AllocationResult(oldFreeListHead, cacheEntry, false);
  }

  @Override
  public int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes) {
    final Map<K, OSBTreeRidBag.Change> notAppliedChanges = new HashMap<K, OSBTreeRidBag.Change>(changes);
    final OModifiableInteger size = new OModifiableInteger(0);
    loadEntriesMajor(firstKey(), true, true, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        final OSBTreeRidBag.Change change = notAppliedChanges.remove(entry.getKey());
        final int result;

        final Integer treeValue = (Integer) entry.getValue();
        if (change == null) {
            result = treeValue;
        } else {
            result = change.applyTo(treeValue);
        }

        size.increment(result);
        return true;
      }
    });

    for (OSBTreeRidBag.Change change : notAppliedChanges.values()) {
      size.increment(change.applyTo(0));
    }

    return size.intValue();
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    acquireSharedLock();
    try {
      return keySerializer;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    acquireSharedLock();
    try {
      return valueSerializer;
    } finally {
      releaseSharedLock();
    }
  }

  private static class AllocationResult {
    private final OBonsaiBucketPointer pointer;
    private final OCacheEntry          cacheEntry;
    private final boolean              newPage;

    private AllocationResult(OBonsaiBucketPointer pointer, OCacheEntry cacheEntry, boolean newPage) {
      this.pointer = pointer;
      this.cacheEntry = cacheEntry;
      this.newPage = newPage;
    }

    private OBonsaiBucketPointer getPointer() {
      return pointer;
    }

    private OCacheEntry getCacheEntry() {
      return cacheEntry;
    }

    private boolean isNewPage() {
      return newPage;
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
}
