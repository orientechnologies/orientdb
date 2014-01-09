/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;
import java.util.*;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * Tree-based dictionary algorithm. Similar to {@link OSBTree} but uses subpages of disk cache that is more efficient for small data
 * structures.
 * 
 * Oriented for usage of several instances inside of one file.
 * 
 * Creation of several instances that represent the same collection is not allowed.
 * 
 * @author Andrey Lomakin
 * @author Artem Orobets
 * @since 1.6.0
 * @see OSBTree
 */
public class OSBTreeBonsai<K, V> extends ODurableComponent implements OTreeInternal<K, V> {
  private static final int                  PAGE_SIZE             = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  private final float                       freeSpaceReuseTrigger = OGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER
                                                                      .getValueAsFloat();
  private static final OBonsaiBucketPointer SYS_BUCKET            = new OBonsaiBucketPointer(0, 0);

  private OBonsaiBucketPointer              rootBucketPointer;

  private final Comparator<? super K>       comparator            = ODefaultComparator.INSTANCE;

  private OStorageLocalAbstract             storage;
  private String                            name;

  private final String                      dataFileExtension;

  private ODiskCache                        diskCache;

  private long                              fileId;

  private OBinarySerializer<K>              keySerializer;
  private OBinarySerializer<V>              valueSerializer;

  private final boolean                     durableInNonTxMode;

  public OSBTreeBonsai(String dataFileExtension, boolean durableInNonTxMode) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
    this.durableInNonTxMode = durableInNonTxMode;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OStorageLocalAbstract storageLocal) {
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

  public void create(long fileId, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OStorageLocalAbstract storageLocal) {
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

      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      rootPointer.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            this.rootBucketPointer.getPageOffset(), true, keySerializer, valueSerializer, getTrackMode());
        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setValueSerializerId(valueSerializer.getId());
        rootBucket.setTreeSize(0);

        super.logPageChanges(rootBucket, fileId, this.rootBucketPointer.getPageIndex(), true);
        rootCacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(rootCacheEntry);
      }

      super.endAtomicOperation(false);
    } catch (IOException e) {
      try {
        super.endAtomicOperation(true);
      } catch (IOException e1) {
        OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
      }
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    }
  }

  private void initDurableComponent(OStorageLocalAbstract storageLocal) {
    final OWriteAheadLog writeAheadLog = storageLocal.getWALInstance();
    final OAtomicOperationsManager atomicOperationsManager = storageLocal.getAtomicOperationsManager();

    init(atomicOperationsManager, writeAheadLog);
  }

  public String getName() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  public long getFileId() {
    acquireSharedLock();
    try {
      return fileId;
    } finally {
      releaseSharedLock();
    }
  }

  public OBonsaiBucketPointer getRootBucketPointer() {
    acquireSharedLock();
    try {
      return rootBucketPointer;
    } finally {
      releaseSharedLock();
    }
  }

  public V get(K key) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();
      try {
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
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

  public boolean put(K key, V value) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      lockTillAtomicOperationCompletes();

      BucketSearchResult bucketSearchResult = findBucket(key);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      keyBucketPointer.acquireExclusiveLock();
      OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

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
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);
          bucketPointer = bucketSearchResult.getLastPathItem();

          insertionIndex = bucketSearchResult.itemIndex;

          keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false);
          keyBucketPointer = keyBucketCacheEntry.getCachePointer();
          keyBucketPointer.acquireExclusiveLock();

          keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketPointer.getPageIndex(), false);
        keyBucketCacheEntry.markDirty();
      }

      keyBucketPointer.releaseExclusiveLock();
      diskCache.release(keyBucketCacheEntry);

      if (!itemFound)
        setSize(size() + 1);

      endAtomicOperation(false);
      return result;
    } catch (IOException e) {
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

  public void clear() {
    acquireExclusiveLock();
    try {
      startAtomicOperation();

      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = cacheEntry.getCachePointer();
      rootPointer.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

        addChildrenToQueue(subTreesToDelete, rootBucket);

        rootBucket.shrink(0);
        rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(), rootBucketPointer.getPageOffset(), true,
            keySerializer, valueSerializer, getTrackMode());

        rootBucket.setTreeSize(0);

        logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), true);
        cacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      recycleSubTrees(subTreesToDelete);

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();

      throw new OSBTreeException("Error during clear of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
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

  private void recycleSubTrees(Queue<OBonsaiBucketPointer> subTreesToDelete) throws IOException {
    OBonsaiBucketPointer head = OBonsaiBucketPointer.NULL;
    OBonsaiBucketPointer tail = subTreesToDelete.peek();

    int bucketCount = 0;
    while (!subTreesToDelete.isEmpty()) {
      final OBonsaiBucketPointer bucketPointer = subTreesToDelete.poll();
      OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer pointer = cacheEntry.getCachePointer();
      pointer.acquireExclusiveLock();
      try {
        final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

        addChildrenToQueue(subTreesToDelete, bucket);

        bucket.setFreeListPointer(head);
        head = bucketPointer;

        logPageChanges(bucket, fileId, bucketPointer.getPageIndex(), false);
        cacheEntry.markDirty();
      } finally {
        pointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }
      bucketCount++;
    }

    if (head.isValid()) {
      final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
      final OCachePointer sysCachePointer = sysCacheEntry.getCachePointer();
      sysCachePointer.acquireExclusiveLock();
      try {
        final OSysBucket sysBucket = new OSysBucket(sysCachePointer.getDataPointer(), getTrackMode());

        attachFreeListHead(tail, sysBucket.getFreeListHead());
        sysBucket.setFreeListHead(head);
        sysBucket.setFreeListLength(sysBucket.freeListLength() + bucketCount);

        logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), false);
        sysCacheEntry.markDirty();
      } finally {
        sysCachePointer.releaseExclusiveLock();
        diskCache.release(sysCacheEntry);
      }
    }
  }

  private void attachFreeListHead(OBonsaiBucketPointer bucketPointer, OBonsaiBucketPointer freeListHead) throws IOException {
    OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
    OCachePointer pointer = cacheEntry.getCachePointer();
    pointer.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

      bucket.setFreeListPointer(freeListHead);

      super.logPageChanges(bucket, fileId, bucketPointer.getPageIndex(), false);
      cacheEntry.markDirty();
    } finally {
      pointer.releaseExclusiveLock();
      diskCache.release(cacheEntry);
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      lockTillAtomicOperationCompletes();

      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();
      subTreesToDelete.add(rootBucketPointer);
      recycleSubTrees(subTreesToDelete);

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();

      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(long fileId, OBonsaiBucketPointer rootBucketPointer, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.rootBucketPointer = rootBucketPointer;

      diskCache = storage.getDiskCache();

      diskCache.openFile(fileId);
      this.fileId = fileId;
      this.name = resolveTreeName(fileId);

      OCacheEntry rootCacheEntry = diskCache.load(this.fileId, this.rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      rootPointer.acquireSharedLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            this.rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getValueSerializerId());
      } finally {
        rootPointer.releaseSharedLock();
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

    OCachePointer rootPointer = rootCacheEntry.getCachePointer();
    rootPointer.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
          rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
      rootBucket.setTreeSize(size);

      logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), false);
      rootCacheEntry.markDirty();
    } finally {
      rootPointer.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      OCacheEntry rootCacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      try {
        OSBTreeBonsaiBucket rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
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

  public V remove(K key) {
    acquireExclusiveLock();
    try {

      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      final V removed;

      keyBucketPointer.acquireExclusiveLock();
      try {
        startAtomicOperation();
        lockTillAtomicOperationCompletes();

        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

        removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

        keyBucket.remove(bucketSearchResult.itemIndex);

        logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
        keyBucketCacheEntry.markDirty();

      } finally {
        keyBucketPointer.releaseExclusiveLock();
        diskCache.release(keyBucketCacheEntry);
      }
      setSize(size() - 1);
      endAtomicOperation(false);
      return removed;

    } catch (IOException e) {
      rollback();

      throw new OSBTreeException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected void endAtomicOperation(boolean rollback) throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.endAtomicOperation(rollback);
  }

  @Override
  protected void startAtomicOperation() throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.startAtomicOperation();
  }

  @Override
  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode)
      return;

    super.logPageChanges(localPage, fileId, pageIndex, isNewPage);
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode)
      return ODurablePage.TrackMode.NONE;

    return super.getTrackMode();
  }

  public Collection<V> getValuesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
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
  }

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
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!firstBucket)
            index = bucket.size() - 1;

          for (int i = index; i >= 0; i--) {
            if (!listener.addResult(bucket.getEntry(i)))
              return;
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

  public Collection<V> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();

    loadEntriesMajor(key, inclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        result.add(entry.getValue());
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

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
   * @param listener
   *          callback that is executed for each entry
   */
  public void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
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
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          int bucketSize = bucket.size();
          for (int i = index; i < bucketSize; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              return;
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

  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, final int maxValuesToFetch) {
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
  }

  public K firstKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      int itemIndex = 0;

      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
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
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(), bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, ODurablePage.TrackMode.NONE);
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

  public K lastKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);

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

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(), bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, ODurablePage.TrackMode.NONE);
          if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1)
            itemIndex = bucket.size() - 1;
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
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!bucketPointer.equals(bucketPointerTo))
            endIndex = bucket.size() - 1;
          else
            endIndex = indexTo;

          for (int i = startIndex; i <= endIndex; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              break resultsLoop;
          }

          if (bucketPointer.equals(bucketPointerTo))
            break;

          bucketPointer = bucket.getRightSibling();
          if (bucketPointer.getPageIndex() < 0)
            break;

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
    OCachePointer pointer = bucketEntry.getCachePointer();

    pointer.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> bucketToSplit = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

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
        final AllocationResult allocationResult = allocateBucket();
        OCacheEntry rightBucketEntry = allocationResult.getCacheEntry();
        final OBonsaiBucketPointer rightBucketPointer = allocationResult.getPointer();

        OCachePointer rightPointer = rightBucketEntry.getCachePointer();

        rightPointer.acquireExclusiveLock();

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightPointer.getDataPointer(),
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
              final OCachePointer rightSiblingPointer = rightSiblingBucketEntry.getCachePointer();

              rightSiblingPointer.acquireExclusiveLock();
              OSBTreeBonsaiBucket<K, V> rightSiblingBucket = new OSBTreeBonsaiBucket<K, V>(rightSiblingPointer.getDataPointer(),
                  rightSiblingBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
                logPageChanges(rightSiblingBucket, fileId, rightSiblingBucketPointer.getPageIndex(), false);

                rightSiblingBucketEntry.markDirty();
              } finally {
                rightSiblingPointer.releaseExclusiveLock();
                diskCache.release(rightSiblingBucketEntry);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);
          OCachePointer parentPointer = parentCacheEntry.getCachePointer();

          parentPointer.acquireExclusiveLock();
          try {
            OSBTreeBonsaiBucket<K, V> parentBucket = new OSBTreeBonsaiBucket<K, V>(parentPointer.getDataPointer(),
                parentBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
            OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(bucketPointer,
                rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentPointer.releaseExclusiveLock();
              diskCache.release(parentCacheEntry);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);
              parentPointer = parentCacheEntry.getCachePointer();

              parentPointer.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBonsaiBucket<K, V>(parentPointer.getDataPointer(), parentBucketPointer.getPageOffset(),
                  keySerializer, valueSerializer, getTrackMode());
            }

            logPageChanges(parentBucket, fileId, parentBucketPointer.getPageIndex(), false);
          } finally {
            parentCacheEntry.markDirty();
            parentPointer.releaseExclusiveLock();

            diskCache.release(parentCacheEntry);
          }

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), allocationResult.isNewPage());
        } finally {
          rightBucketEntry.markDirty();
          rightPointer.releaseExclusiveLock();
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

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        final AllocationResult leftAllocationResult = allocateBucket();
        OCacheEntry leftBucketEntry = leftAllocationResult.getCacheEntry();
        OBonsaiBucketPointer leftBucketPointer = leftAllocationResult.getPointer();
        OCachePointer leftPointer = leftBucketEntry.getCachePointer();

        final AllocationResult rightAllocationResult = allocateBucket();
        OCacheEntry rightBucketEntry = rightAllocationResult.getCacheEntry();
        OBonsaiBucketPointer rightBucketPointer = rightAllocationResult.getPointer();
        leftPointer.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newLeftBucket = new OSBTreeBonsaiBucket<K, V>(leftPointer.getDataPointer(),
              leftBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketPointer);

          logPageChanges(newLeftBucket, fileId, leftBucketEntry.getPageIndex(), leftAllocationResult.isNewPage());
          leftBucketEntry.markDirty();
        } finally {
          leftPointer.releaseExclusiveLock();
          diskCache.release(leftBucketEntry);
        }

        OCachePointer rightPointer = rightBucketEntry.getCachePointer();
        rightPointer.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightPointer.getDataPointer(),
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketPointer);

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), rightAllocationResult.isNewPage());
          rightBucketEntry.markDirty();
        } finally {
          rightPointer.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        bucketToSplit = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(), false,
            keySerializer, valueSerializer, getTrackMode());
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

        if (splitLeaf)
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
      }

    } finally {
      bucketEntry.markDirty();
      pointer.releaseExclusiveLock();
      diskCache.release(bucketEntry);
    }
  }

  private BucketSearchResult findBucket(K key) throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();

    while (true) {
      path.add(bucketPointer);
      final OCacheEntry bucketEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      final OCachePointer pointer = bucketEntry.getCachePointer();

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
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
        diskCache.release(bucketEntry);
      }

      if (comparator.compare(key, entry.key) >= 0)
        bucketPointer = entry.rightChild;
      else
        bucketPointer = entry.leftChild;
    }
  }

  private void initSysBucket() throws IOException {
    final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
    final OCachePointer cachePointer = sysCacheEntry.getCachePointer();
    cachePointer.acquireExclusiveLock();
    try {
      final OSysBucket sysBucket = new OSysBucket(cachePointer.getDataPointer(), getTrackMode());
      if (sysBucket.isInitialized()) {
        super.startAtomicOperation();

        sysBucket.init();
        super.logPageChanges(sysBucket, fileId, SYS_BUCKET.getPageIndex(), true);
        sysCacheEntry.markDirty();

        super.endAtomicOperation(false);
      }
    } finally {
      cachePointer.releaseExclusiveLock();
      diskCache.release(sysCacheEntry);
    }
  }

  private AllocationResult allocateBucket() throws IOException {
    final OCacheEntry sysCacheEntry = diskCache.load(fileId, SYS_BUCKET.getPageIndex(), false);
    final OCachePointer cachePointer = sysCacheEntry.getCachePointer();
    cachePointer.acquireExclusiveLock();
    try {
      final OSysBucket sysBucket = new OSysBucket(cachePointer.getDataPointer(), getTrackMode());
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
          sysCacheEntry.markDirty();

          return new AllocationResult(freeSpacePointer, cacheEntry, false);
        }
      }
    } finally {
      cachePointer.releaseExclusiveLock();
      diskCache.release(sysCacheEntry);
    }
  }

  private AllocationResult reuseBucketFromFreeList(OSysBucket sysBucket) throws IOException {
    final OBonsaiBucketPointer oldFreeListHead = sysBucket.getFreeListHead();
    assert oldFreeListHead.isValid();

    OCacheEntry cacheEntry = diskCache.load(fileId, oldFreeListHead.getPageIndex(), false);
    OCachePointer pointer = cacheEntry.getCachePointer();
    pointer.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
          oldFreeListHead.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

      sysBucket.setFreeListHead(bucket.getFreeListPointer());
      sysBucket.setFreeListLength(sysBucket.freeListLength() - 1);

      logPageChanges(bucket, fileId, oldFreeListHead.getPageIndex(), false);
      cacheEntry.markDirty();
    } finally {
      pointer.releaseExclusiveLock();
    }
    return new AllocationResult(oldFreeListHead, cacheEntry, false);
  }

  /**
   * Hardcoded method for Bag to avoid creation of extra layer.
   * 
   * Don't make any changes to tree.
   * 
   * @param changes
   *          Bag changes
   * @return real bag size
   */
  public int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes) {
    final Map<K, OSBTreeRidBag.Change> notAppliedChanges = new HashMap<K, OSBTreeRidBag.Change>(changes);
    final OModifiableInteger size = new OModifiableInteger(0);
    loadEntriesMajor(firstKey(), true, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(Map.Entry<K, V> entry) {
        final OSBTreeRidBag.Change change = notAppliedChanges.remove(entry.getKey());
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

    for (OSBTreeRidBag.Change change : notAppliedChanges.values()) {
      size.increment(change.applyTo(0));
    }

    return size.intValue();
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
