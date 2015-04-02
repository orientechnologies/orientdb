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
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

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

  private String                            name;

  private final String                      dataFileExtension;

  private ODiskCache                        diskCache;

  private long                              fileId;

  private OBinarySerializer<K>              keySerializer;
  private OBinarySerializer<V>              valueSerializer;

  private final boolean                     durableInNonTxMode;

  public OSBTreeBonsaiLocal(String dataFileExtension, boolean durableInNonTxMode, OAbstractPaginatedStorage storage) {
    super(storage);
    this.dataFileExtension = dataFileExtension;
    this.durableInNonTxMode = durableInNonTxMode;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree creation.", e);
    }

    acquireExclusiveLock();
    try {
      this.diskCache = storage.getDiskCache();

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      if (isFileExists(atomicOperation, name + dataFileExtension, diskCache))
        this.fileId = openFile(atomicOperation, name + dataFileExtension, diskCache);
      else
        this.fileId = addFile(atomicOperation, name + dataFileExtension, diskCache);

      this.name = name;

      initAfterCreate(atomicOperation);

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } catch (Exception e) {
      rollback();
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(long fileId, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree creation.", e);
    }

    acquireExclusiveLock();
    try {
      this.diskCache = storage.getDiskCache();

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      openFile(atomicOperation, fileId, diskCache);

      this.fileId = fileId;
      this.name = resolveTreeName(fileId, atomicOperation);

      initAfterCreate(atomicOperation);

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();

      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } catch (Exception e) {
      rollback();

      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void initAfterCreate(OAtomicOperation atomicOperation) throws IOException {
    initSysBucket(atomicOperation);

    final AllocationResult allocationResult = allocateBucket(atomicOperation);
    OCacheEntry rootCacheEntry = allocationResult.getCacheEntry();
    this.rootBucketPointer = allocationResult.getPointer();

    rootCacheEntry.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, this.rootBucketPointer.getPageOffset(),
          true, keySerializer, valueSerializer, getChangesTree(atomicOperation, rootCacheEntry));
      rootBucket.setTreeSize(0);
    } finally {
      rootCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, rootCacheEntry, diskCache);
    }
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
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return new OBonsaiCollectionPointer(fileId, rootBucketPointer);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public V get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        if (bucketSearchResult.itemIndex < 0)
          return null;

        OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

        OCacheEntry keyBucketCacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
        try {
          OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, getChangesTree(atomicOperation, keyBucketCacheEntry));
          return keyBucket.getEntry(bucketSearchResult.itemIndex).value;
        } finally {
          releasePage(atomicOperation, keyBucketCacheEntry, diskCache);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving  of sbtree with name " + name, e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean put(K key, V value) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree entrie put.", e);
    }

    acquireExclusiveLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
      keyBucketCacheEntry.acquireExclusiveLock();
      OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getChangesTree(atomicOperation, keyBucketCacheEntry));

      final boolean itemFound = bucketSearchResult.itemIndex >= 0;
      boolean result = true;
      if (itemFound) {
        final int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, value);
        assert updateResult == 0 || updateResult == 1;

        result = updateResult != 0;
      } else {
        int insertionIndex = -bucketSearchResult.itemIndex - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL,
            OBonsaiBucketPointer.NULL, key, value), true)) {
          keyBucketCacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, keyBucketCacheEntry, diskCache);

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);
          bucketPointer = bucketSearchResult.getLastPathItem();

          insertionIndex = bucketSearchResult.itemIndex;

          keyBucketCacheEntry = loadPage(atomicOperation, fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false,
              diskCache);
          keyBucketCacheEntry.acquireExclusiveLock();

          keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, getChangesTree(atomicOperation, keyBucketCacheEntry));
        }
      }

      keyBucketCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, keyBucketCacheEntry, diskCache);

      if (!itemFound)
        setSize(size() + 1, atomicOperation);

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
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree entrie clear.", e);
    }

    acquireExclusiveLock();
    try {
      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();

      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false, diskCache);
      cacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));

        addChildrenToQueue(subTreesToDelete, rootBucket);

        rootBucket.shrink(0);
        rootBucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, rootBucketPointer.getPageOffset(), true, keySerializer,
            valueSerializer, getChangesTree(atomicOperation, cacheEntry));

        rootBucket.setTreeSize(0);
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry, diskCache);
      }

      recycleSubTrees(subTreesToDelete, atomicOperation);

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
      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
      cacheEntry.acquireExclusiveLock();
      try {
        final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));

        addChildrenToQueue(subTreesToDelete, bucket);

        bucket.setFreeListPointer(head);
        head = bucketPointer;
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry, diskCache);
      }
      bucketCount++;
    }

    if (head.isValid()) {
      final OCacheEntry sysCacheEntry = loadPage(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false, diskCache);
      sysCacheEntry.acquireExclusiveLock();
      try {
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getChangesTree(atomicOperation, sysCacheEntry));

        attachFreeListHead(tail, sysBucket.getFreeListHead(), atomicOperation);
        sysBucket.setFreeListHead(head);
        sysBucket.setFreeListLength(sysBucket.freeListLength() + bucketCount);

      } finally {
        sysCacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, sysCacheEntry, diskCache);
      }
    }
  }

  private void attachFreeListHead(OBonsaiBucketPointer bucketPointer, OBonsaiBucketPointer freeListHead,
      OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
    cacheEntry.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));

      bucket.setFreeListPointer(freeListHead);
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry, diskCache);
    }
  }

  /**
   * Deletes a whole tree. Puts all its pages to free list for further reusage.
   */
  @Override
  public void delete() {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree deletion.", e);
    }

    acquireExclusiveLock();
    try {
      final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<OBonsaiBucketPointer>();
      subTreesToDelete.add(rootBucketPointer);
      recycleSubTrees(subTreesToDelete, atomicOperation);

      endAtomicOperation(false);
    } catch (Throwable e) {
      rollback();

      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(long fileId, OBonsaiBucketPointer rootBucketPointer) {
    acquireExclusiveLock();
    try {
      this.rootBucketPointer = rootBucketPointer;

      diskCache = storage.getDiskCache();

      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      openFile(atomicOperation, fileId, diskCache);

      this.fileId = fileId;
      this.name = resolveTreeName(fileId, atomicOperation);

      OCacheEntry rootCacheEntry = loadPage(atomicOperation, this.fileId, this.rootBucketPointer.getPageIndex(), false, diskCache);

      rootCacheEntry.acquireSharedLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry,
            this.rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, getChangesTree(atomicOperation, rootCacheEntry));
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            rootBucket.getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            rootBucket.getValueSerializerId());
      } finally {
        rootCacheEntry.releaseSharedLock();
        releasePage(atomicOperation, rootCacheEntry, diskCache);
      }

    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + fileId, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private String resolveTreeName(long fileId, OAtomicOperation atomicOperation) {
    final String fileName = fileNameById(atomicOperation, fileId, diskCache);
    return fileName.substring(0, fileName.length() - dataFileExtension.length());
  }

  private void setSize(long size, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPage(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false, diskCache);

    rootCacheEntry.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getChangesTree(atomicOperation, rootCacheEntry));
      rootBucket.setTreeSize(size);
    } finally {
      rootCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, rootCacheEntry, diskCache);
    }
  }

  @Override
  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        OCacheEntry rootCacheEntry = loadPage(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false, diskCache);

        try {
          OSBTreeBonsaiBucket rootBucket = new OSBTreeBonsaiBucket<K, V>(rootCacheEntry, rootBucketPointer.getPageOffset(),
              keySerializer, valueSerializer, getChangesTree(atomicOperation, rootCacheEntry));
          return rootBucket.getTreeSize();
        } finally {
          releasePage(atomicOperation, rootCacheEntry, diskCache);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving of size of index " + name);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public V remove(K key) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OSBTreeException("Error during sbtree entrie removal.", e);
    }

    acquireExclusiveLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
      if (bucketSearchResult.itemIndex < 0) {
        endAtomicOperation(false);
        return null;
      }

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
      final V removed;

      keyBucketCacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketCacheEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getChangesTree(atomicOperation, keyBucketCacheEntry));

        removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

        keyBucket.remove(bucketSearchResult.itemIndex);
      } finally {
        keyBucketCacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, keyBucketCacheEntry, diskCache);
      }
      setSize(size() - 1, atomicOperation);

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
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.endAtomicOperation(rollback);
  }

  @Override
  protected OAtomicOperation startAtomicOperation() throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return atomicOperationsManager.getCurrentOperation();

    return super.startAtomicOperation();
  }

  @Override
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

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
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
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
          try {
            OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));
            if (!firstBucket)
              index = bucket.size() - 1;

            for (int i = index; i >= 0; i--) {
              if (!listener.addResult(bucket.getEntry(i)))
                return;
            }

            bucketPointer = bucket.getLeftSibling();

            firstBucket = false;

          } finally {
            releasePage(atomicOperation, cacheEntry, diskCache);
          }
        } while (bucketPointer.getPageIndex() >= 0);
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of minor values for key " + key + " in sbtree " + name);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
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
    if (!ascSortOrder)
      throw new IllegalStateException("Descending sort order is not supported.");

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
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
          final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
          try {
            OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));
            int bucketSize = bucket.size();
            for (int i = index; i < bucketSize; i++) {
              if (!listener.addResult(bucket.getEntry(i)))
                return;
            }

            bucketPointer = bucket.getRightSibling();
            index = 0;
          } finally {
            releasePage(atomicOperation, cacheEntry, diskCache);
          }

        } while (bucketPointer.getPageIndex() >= 0);
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of major values for key " + key + " in sbtree " + name);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
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

  @Override
  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

        OBonsaiBucketPointer bucketPointer = rootBucketPointer;

        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, rootBucketPointer.getPageIndex(), false, diskCache);
        int itemIndex = 0;

        OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer,
            valueSerializer, getChangesTree(atomicOperation, cacheEntry));
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

            releasePage(atomicOperation, cacheEntry, diskCache);
            cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);

            bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
                getChangesTree(atomicOperation, cacheEntry));
          }
        } finally {
          releasePage(atomicOperation, cacheEntry, diskCache);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

        OBonsaiBucketPointer bucketPointer = rootBucketPointer;

        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
        OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer,
            valueSerializer, getChangesTree(atomicOperation, cacheEntry));

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

            releasePage(atomicOperation, cacheEntry, diskCache);
            cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);

            bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer,
                getChangesTree(atomicOperation, cacheEntry));
            if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1)
              itemIndex = bucket.size() - 1;
          }
        } finally {
          releasePage(atomicOperation, cacheEntry, diskCache);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
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

        resultsLoop: while (true) {

          final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);
          try {
            OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, bucketPointer.getPageOffset(),
                keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));
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
            releasePage(atomicOperation, cacheEntry, diskCache);
          }

          startIndex = 0;
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + name);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void flush() {
    atomicOperationsManager.acquireReadLock(this);
    try {
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
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private BucketSearchResult splitBucket(List<OBonsaiBucketPointer> path, int keyIndex, K keyToInsert,
      OAtomicOperation atomicOperation) throws IOException {
    final OBonsaiBucketPointer bucketPointer = path.get(path.size() - 1);

    OCacheEntry bucketEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);

    bucketEntry.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
          keySerializer, valueSerializer, getChangesTree(atomicOperation, bucketEntry));

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
        final AllocationResult allocationResult = allocateBucket(atomicOperation);
        OCacheEntry rightBucketEntry = allocationResult.getCacheEntry();
        final OBonsaiBucketPointer rightBucketPointer = allocationResult.getPointer();
        rightBucketEntry.acquireExclusiveLock();

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getChangesTree(atomicOperation,
                  rightBucketEntry));
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            OBonsaiBucketPointer rightSiblingBucketPointer = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingBucketPointer);
            newRightBucket.setLeftSibling(bucketPointer);

            bucketToSplit.setRightSibling(rightBucketPointer);

            if (rightSiblingBucketPointer.isValid()) {
              final OCacheEntry rightSiblingBucketEntry = loadPage(atomicOperation, fileId,
                  rightSiblingBucketPointer.getPageIndex(), false, diskCache);

              rightSiblingBucketEntry.acquireExclusiveLock();
              OSBTreeBonsaiBucket<K, V> rightSiblingBucket = new OSBTreeBonsaiBucket<K, V>(rightSiblingBucketEntry,
                  rightSiblingBucketPointer.getPageOffset(), keySerializer, valueSerializer, getChangesTree(atomicOperation,
                      rightSiblingBucketEntry));
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
              } finally {
                rightSiblingBucketEntry.releaseExclusiveLock();
                releasePage(atomicOperation, rightSiblingBucketEntry, diskCache);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = loadPage(atomicOperation, fileId, parentBucketPointer.getPageIndex(), false, diskCache);

          parentCacheEntry.acquireExclusiveLock();
          try {
            OSBTreeBonsaiBucket<K, V> parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry,
                parentBucketPointer.getPageOffset(), keySerializer, valueSerializer, getChangesTree(atomicOperation,
                    parentCacheEntry));
            OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(bucketPointer,
                rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentCacheEntry.releaseExclusiveLock();
              releasePage(atomicOperation, parentCacheEntry, diskCache);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey,
                  atomicOperation);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry = loadPage(atomicOperation, fileId, parentBucketPointer.getPageIndex(), false, diskCache);

              parentCacheEntry.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBonsaiBucket<K, V>(parentCacheEntry, parentBucketPointer.getPageOffset(), keySerializer,
                  valueSerializer, getChangesTree(atomicOperation, parentCacheEntry));
            }

          } finally {
            parentCacheEntry.releaseExclusiveLock();

            releasePage(atomicOperation, parentCacheEntry, diskCache);
          }

        } finally {
          rightBucketEntry.releaseExclusiveLock();
          releasePage(atomicOperation, rightBucketEntry, diskCache);
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

        final AllocationResult leftAllocationResult = allocateBucket(atomicOperation);
        OCacheEntry leftBucketEntry = leftAllocationResult.getCacheEntry();
        OBonsaiBucketPointer leftBucketPointer = leftAllocationResult.getPointer();

        final AllocationResult rightAllocationResult = allocateBucket(atomicOperation);
        OCacheEntry rightBucketEntry = rightAllocationResult.getCacheEntry();
        OBonsaiBucketPointer rightBucketPointer = rightAllocationResult.getPointer();
        leftBucketEntry.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newLeftBucket = new OSBTreeBonsaiBucket<K, V>(leftBucketEntry,
              leftBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getChangesTree(atomicOperation,
                  leftBucketEntry));
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketPointer);
        } finally {
          leftBucketEntry.releaseExclusiveLock();
          releasePage(atomicOperation, leftBucketEntry, diskCache);
        }

        rightBucketEntry.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightBucketEntry,
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getChangesTree(atomicOperation,
                  rightBucketEntry));
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketPointer);
        } finally {
          rightBucketEntry.releaseExclusiveLock();
          releasePage(atomicOperation, rightBucketEntry, diskCache);
        }

        bucketToSplit = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(), false, keySerializer,
            valueSerializer, getChangesTree(atomicOperation, bucketEntry));
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit.addEntry(0, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(leftBucketPointer, rightBucketPointer, separationKey,
            null), true);

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
      bucketEntry.releaseExclusiveLock();
      releasePage(atomicOperation, bucketEntry, diskCache);
    }
  }

  private BucketSearchResult findBucket(K key, OAtomicOperation atomicOperation) throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();

    while (true) {
      path.add(bucketPointer);
      final OCacheEntry bucketEntry = loadPage(atomicOperation, fileId, bucketPointer.getPageIndex(), false, diskCache);

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(bucketEntry, bucketPointer.getPageOffset(),
            keySerializer, valueSerializer, getChangesTree(atomicOperation, bucketEntry));
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
        releasePage(atomicOperation, bucketEntry, diskCache);
      }

      if (comparator.compare(key, entry.key) >= 0)
        bucketPointer = entry.rightChild;
      else
        bucketPointer = entry.leftChild;
    }
  }

  private void initSysBucket(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry sysCacheEntry = loadPage(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false, diskCache);
    if (sysCacheEntry == null) {
      sysCacheEntry = addPage(atomicOperation, fileId, diskCache);
      assert sysCacheEntry.getPageIndex() == SYS_BUCKET.getPageIndex();
    }

    sysCacheEntry.acquireExclusiveLock();
    try {
      OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getChangesTree(atomicOperation, sysCacheEntry));
      if (sysBucket.isInitialized()) {
        sysCacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, sysCacheEntry, diskCache);

        sysCacheEntry = loadPage(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false, diskCache);
        sysCacheEntry.acquireExclusiveLock();

        sysBucket = new OSysBucket(sysCacheEntry, getChangesTree(atomicOperation, sysCacheEntry));
        sysBucket.init();
      }
    } finally {
      sysCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, sysCacheEntry, diskCache);
    }
  }

  private AllocationResult allocateBucket(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry sysCacheEntry = loadPage(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), false, diskCache);
    if (sysCacheEntry == null) {
      sysCacheEntry = addPage(atomicOperation, fileId, diskCache);
      assert sysCacheEntry.getPageIndex() == SYS_BUCKET.getPageIndex();
    }

    sysCacheEntry.acquireExclusiveLock();
    try {
      final OSysBucket sysBucket = new OSysBucket(sysCacheEntry, getChangesTree(atomicOperation, sysCacheEntry));
      if ((1.0 * sysBucket.freeListLength())
          / (getFilledUpTo(atomicOperation, diskCache, fileId) * PAGE_SIZE / OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES) >= freeSpaceReuseTrigger) {
        final AllocationResult allocationResult = reuseBucketFromFreeList(sysBucket, atomicOperation);
        return allocationResult;
      } else {
        final OBonsaiBucketPointer freeSpacePointer = sysBucket.getFreeSpacePointer();
        if (freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES > PAGE_SIZE) {
          final OCacheEntry cacheEntry = addPage(atomicOperation, fileId, diskCache);
          final long pageIndex = cacheEntry.getPageIndex();
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(pageIndex, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));

          return new AllocationResult(new OBonsaiBucketPointer(pageIndex, 0), cacheEntry, true);
        } else {
          sysBucket.setFreeSpacePointer(new OBonsaiBucketPointer(freeSpacePointer.getPageIndex(), freeSpacePointer.getPageOffset()
              + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
          final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, freeSpacePointer.getPageIndex(), false, diskCache);

          return new AllocationResult(freeSpacePointer, cacheEntry, false);
        }
      }
    } finally {
      sysCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, sysCacheEntry, diskCache);
    }
  }

  private AllocationResult reuseBucketFromFreeList(OSysBucket sysBucket, OAtomicOperation atomicOperation) throws IOException {
    final OBonsaiBucketPointer oldFreeListHead = sysBucket.getFreeListHead();
    assert oldFreeListHead.isValid();

    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, oldFreeListHead.getPageIndex(), false, diskCache);
    cacheEntry.acquireExclusiveLock();
    try {
      final OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cacheEntry, oldFreeListHead.getPageOffset(),
          keySerializer, valueSerializer, getChangesTree(atomicOperation, cacheEntry));

      sysBucket.setFreeListHead(bucket.getFreeListPointer());
      sysBucket.setFreeListLength(sysBucket.freeListLength() - 1);
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
