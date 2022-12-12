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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OSBTreeBonsaiLocalException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Tree-based dictionary algorithm. Similar to {@link OSBTreeV1} but uses subpages of disk cache
 * that is more efficient for small data structures. Oriented for usage of several instances inside
 * of one file. Creation of several instances that represent the same collection is not allowed.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @author Artem Orobets
 * @see OSBTreeV1
 * @since 1.6.0
 */
public class OSBTreeBonsaiLocal<K, V> extends ODurableComponent implements OSBTreeBonsai<K, V> {
  private static final OLockManager<Long> FILE_LOCK_MANAGER = new OPartitionedLockManager<>();

  private static final int PAGE_SIZE =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  private static final OBonsaiBucketPointer SYS_BUCKET = new OBonsaiBucketPointer(0, 0);

  private OBonsaiBucketPointer rootBucketPointer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private volatile Long fileId = -1L;

  private OBinarySerializer<K> keySerializer;
  private OBinarySerializer<V> valueSerializer;

  public OSBTreeBonsaiLocal(
      final String name, final String dataFileExtension, final OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
  }

  public long createComponent(OAtomicOperation atomicOperation) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          if (isFileExists(atomicOperation, getFullName())) {
            throw new OStorageException(
                "Ridbag component with name " + getFullName() + " already exists");
          } else {
            this.fileId = addFile(atomicOperation, getFullName());
          }

          initSysBucket(atomicOperation);

          return fileId;
        });
  }

  public void create(
      final OAtomicOperation atomicOperation,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer)
      throws IOException {

    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(-1L);
          try {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (isFileExists(atomicOperation, getFullName())) {
              this.fileId = openFile(atomicOperation, getFullName());
            } else {
              throw new OStorageException(
                  "Ridbag component with name " + getFullName() + " does not exist");
            }

            initAfterCreate(atomicOperation, -1, -1, atomicOperation.getDeletedBonsaiPointers());
          } finally {
            lock.unlock();
          }
        });
  }

  public void create(
      final OAtomicOperation atomicOperation,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer,
      final int pageIndex,
      final int pageOffset)
      throws IOException {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(-1L);
          try {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            if (isFileExists(atomicOperation, getFullName())) {
              this.fileId = openFile(atomicOperation, getFullName());
            } else {
              throw new OStorageException(
                  "Ridbag component with name " + getFullName() + " does not exist");
            }

            initAfterCreate(atomicOperation, pageIndex, pageOffset, Collections.emptySet());
          } finally {
            lock.unlock();
          }
        });
  }

  private void initAfterCreate(
      final OAtomicOperation atomicOperation,
      final int pageIndex,
      final int pageOffset,
      Set<OBonsaiBucketPointer> blockedPointers)
      throws IOException {

    final AllocationResult allocationResult =
        allocateBucketForWrite(atomicOperation, pageIndex, pageOffset, blockedPointers);
    this.rootBucketPointer = allocationResult.getPointer();

    try (final OCacheEntry rootCacheEntry = allocationResult.getCacheEntry()) {
      final OSBTreeBonsaiBucket<K, V> rootBucket =
          new OSBTreeBonsaiBucket<>(
              rootCacheEntry,
              this.rootBucketPointer.getPageOffset(),
              true,
              keySerializer,
              valueSerializer,
              this);
      rootBucket.setTreeSize(0);
    }

    try (final OCacheEntry sysCacheEntry =
        loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), true)) {
      final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
      sysBucket.incrementTreesCount();
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
  public V get(final K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        if (bucketSearchResult.itemIndex < 0) {
          return null;
        }

        final OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

        try (final OCacheEntry keyBucketCacheEntry =
            loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex())) {
          final OSBTreeBonsaiBucket<K, V> keyBucket =
              new OSBTreeBonsaiBucket<>(
                  keyBucketCacheEntry,
                  bucketPointer.getPageOffset(),
                  keySerializer,
                  valueSerializer,
                  this);
          return keyBucket.getEntry(bucketSearchResult.itemIndex).value;
        }
      } finally {
        lock.unlock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during retrieving  of sbtree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean put(final OAtomicOperation atomicOperation, final K key, final V value) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
            OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

            OCacheEntry keyBucketCacheEntry =
                loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), true);
            OSBTreeBonsaiBucket<K, V> keyBucket =
                new OSBTreeBonsaiBucket<>(
                    keyBucketCacheEntry,
                    bucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);

            final boolean itemFound = bucketSearchResult.itemIndex >= 0;
            boolean result = true;
            if (itemFound) {
              final int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, value);
              assert updateResult == 0 || updateResult == 1;

              result = updateResult != 0;
            } else {
              int insertionIndex = -bucketSearchResult.itemIndex - 1;

              while (!keyBucket.addEntry(
                  insertionIndex,
                  new OSBTreeBonsaiBucket.SBTreeEntry<>(
                      OBonsaiBucketPointer.NULL, OBonsaiBucketPointer.NULL, key, value),
                  true)) {
                keyBucketCacheEntry.close();

                bucketSearchResult =
                    splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);
                bucketPointer = bucketSearchResult.getLastPathItem();

                insertionIndex = bucketSearchResult.itemIndex;

                keyBucketCacheEntry =
                    loadPageForWrite(
                        atomicOperation,
                        fileId,
                        bucketSearchResult.getLastPathItem().getPageIndex(),
                        true);

                keyBucket =
                    new OSBTreeBonsaiBucket<>(
                        keyBucketCacheEntry,
                        bucketPointer.getPageOffset(),
                        keySerializer,
                        valueSerializer,
                        this);
              }
            }

            keyBucketCacheEntry.close();

            if (!itemFound) {
              updateSize(1, atomicOperation);
            }
            return result;
          } finally {
            lock.unlock();
          }
        });
  }

  public List<OBonsaiBucketPointer> loadRoots() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

        final HashSet<Long> children = new HashSet<Long>();
        final HashSet<Long> roots = new HashSet<Long>();

        for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {

          try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
            for (int bucketOffset = pageIndex > 0 ? 0 : OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES;
                bucketOffset < OSBTreeBonsaiBucket.MAX_PAGE_SIZE_BYTES;
                bucketOffset += OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES) {
              final OSBTreeBonsaiBucket<K, V> bucket =
                  new OSBTreeBonsaiBucket<>(
                      cacheEntry, bucketOffset, keySerializer, valueSerializer, this);

              if (!bucket.isLeaf()) {
                for (int bucketIndex = 0; bucketIndex < bucket.size(); bucketIndex++) {
                  final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(bucketIndex);

                  final long leftPointer =
                      (entry.leftChild.getPageIndex() << 16) + entry.leftChild.getPageOffset();
                  final long rightPointer =
                      (entry.rightChild.getPageIndex() << 16) + entry.rightChild.getPageOffset();

                  roots.remove(leftPointer);
                  roots.remove(rightPointer);

                  children.add(leftPointer);
                  children.add(rightPointer);
                }
              }

              final long bucketPointer = (((long) pageIndex) << 16) + bucketOffset;
              if (!children.contains(bucketPointer)) {
                roots.add(bucketPointer);
              }
            }
          }
        }

        final List<OBonsaiBucketPointer> rootPointers = new ArrayList<>(roots.size());
        roots.forEach(
            root -> {
              final int rootPageOffset = (int) (root & 0xFFFF);
              final int rootPageIndex = (int) (root >>> 16);

              rootPointers.add(new OBonsaiBucketPointer(rootPageIndex, rootPageOffset));
            });

        return rootPointers;
      } finally {
        lock.unlock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void forEachItem(
      final OBonsaiBucketPointer rootBucketPointer, final Consumer<ORawPair<K, V>> consumer)
      throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final OBonsaiBucketPointer firstBucket =
            findFirstBucket(rootBucketPointer, atomicOperation);
        if (firstBucket == null) {
          return;
        }

        OBonsaiBucketPointer pointer = firstBucket;
        while (pointer.getPageIndex() >= 0) {

          try (final OCacheEntry cacheEntry =
              loadPageForRead(atomicOperation, fileId, pointer.getPageIndex())) {
            final OSBTreeBonsaiBucket<K, V> bucket =
                new OSBTreeBonsaiBucket<>(
                    cacheEntry, pointer.getPageOffset(), keySerializer, valueSerializer, this);
            assert bucket.isLeaf();

            final int bucketSize = bucket.size();
            for (int entryIndex = 0; entryIndex < bucketSize; entryIndex++) {
              final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(entryIndex);
              consumer.accept(new ORawPair<>(entry.key, entry.value));
            }

            pointer = bucket.getRightSibling();
          }
        }

      } finally {
        lock.unlock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void close(final boolean flush) {
    final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
    try {
      readCache.closeFile(fileId, flush, writeCache);
    } finally {
      lock.unlock();
    }
  }

  public void close() {
    close(true);
  }

  /**
   * Removes all entries from bonsai tree. Put all but the root page to free list for further reuse.
   */
  @Override
  public void clear(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<>();

            try (final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, rootBucketPointer.getPageIndex(), true)) {
              OSBTreeBonsaiBucket<K, V> rootBucket =
                  new OSBTreeBonsaiBucket<>(
                      cacheEntry,
                      rootBucketPointer.getPageOffset(),
                      keySerializer,
                      valueSerializer,
                      this);

              addChildrenToQueue(subTreesToDelete, rootBucket);

              rootBucket.shrink(0);
              rootBucket =
                  new OSBTreeBonsaiBucket<>(
                      cacheEntry,
                      rootBucketPointer.getPageOffset(),
                      true,
                      keySerializer,
                      valueSerializer,
                      this);

              rootBucket.setTreeSize(0);
            }

            recycleSubTrees(subTreesToDelete, atomicOperation);
          } finally {
            lock.unlock();
          }
        });
  }

  private void addChildrenToQueue(
      final Queue<OBonsaiBucketPointer> subTreesToDelete,
      final OSBTreeBonsaiBucket<K, V> rootBucket) {
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

  private void recycleSubTrees(
      final Queue<OBonsaiBucketPointer> subTreesToDelete, final OAtomicOperation atomicOperation)
      throws IOException {
    OBonsaiBucketPointer head = OBonsaiBucketPointer.NULL;
    final OBonsaiBucketPointer tail = subTreesToDelete.peek();

    int bucketCount = 0;
    while (!subTreesToDelete.isEmpty()) {
      final OBonsaiBucketPointer bucketPointer = subTreesToDelete.poll();
      try (final OCacheEntry cacheEntry =
          loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), true)) {
        final OSBTreeBonsaiBucket<K, V> bucket =
            new OSBTreeBonsaiBucket<>(
                cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);

        addChildrenToQueue(subTreesToDelete, bucket);

        bucket.setFreeListPointer(head);
        bucket.setDelted(true);
        head = bucketPointer;
      }
      bucketCount++;
    }

    if (head.isValid()) {
      try (final OCacheEntry sysCacheEntry =
          loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), true)) {
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);

        assert tail != null;
        attachFreeListHead(tail, sysBucket.getFreeListHead(), atomicOperation);
        sysBucket.setFreeListHead(head);
        sysBucket.setFreeListLength(sysBucket.freeListLength() + bucketCount);
      }
    }
  }

  private void attachFreeListHead(
      final OBonsaiBucketPointer bucketPointer,
      final OBonsaiBucketPointer freeListHead,
      final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), true)) {
      final OSBTreeBonsaiBucket<K, V> bucket =
          new OSBTreeBonsaiBucket<>(
              cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);

      bucket.setFreeListPointer(freeListHead);
    }
  }

  /** Deletes a whole tree. Puts all its pages to free list for further reusage. */
  @Override
  public void delete(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            final long size = size();
            if (size > 0) {
              throw new NotEmptyComponentCanNotBeRemovedException(
                  "Ridbag "
                      + getName()
                      + ":"
                      + rootBucketPointer.getPageIndex()
                      + ":"
                      + rootBucketPointer.getPageOffset()
                      + " can not be removed, because it is not empty. Its size is "
                      + size);
            }

            final Queue<OBonsaiBucketPointer> subTreesToDelete = new LinkedList<>();
            subTreesToDelete.add(rootBucketPointer);
            recycleSubTrees(subTreesToDelete, atomicOperation);

            try (final OCacheEntry sysCacheEntry =
                loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), true)) {
              final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
              sysBucket.decrementTreesCount();
            }
          } finally {
            lock.unlock();
          }

          atomicOperation.addDeletedRidBag(rootBucketPointer);
        });
  }

  public void deleteComponent(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          this.fileId = openFile(atomicOperation, getFullName());

          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            final int treesCount;

            try (final OCacheEntry sysCacheEntry =
                loadPageForRead(atomicOperation, fileId, SYS_BUCKET.getPageIndex())) {
              final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
              treesCount = sysBucket.getTreesCount();
            }

            assert treesCount >= 0;

            if (treesCount > 0) {
              throw new NotEmptyComponentCanNotBeRemovedException(
                  "Component "
                      + getName()
                      + " can not be removed because it still contains "
                      + treesCount
                      + " ridbags");
            }

            deleteFile(atomicOperation, fileId);
          } finally {
            lock.unlock();
          }
        });
  }

  public boolean load(final OBonsaiBucketPointer rootBucketPointer) {
    final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
    try {
      this.rootBucketPointer = rootBucketPointer;

      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      this.fileId = openFile(atomicOperation, getFullName());

      try (final OCacheEntry rootCacheEntry =
          loadPageForRead(atomicOperation, this.fileId, this.rootBucketPointer.getPageIndex())) {
        final OSBTreeBonsaiBucket<K, V> rootBucket =
            new OSBTreeBonsaiBucket<>(
                rootCacheEntry,
                this.rootBucketPointer.getPageOffset(),
                keySerializer,
                valueSerializer,
                this);
        //noinspection unchecked
        keySerializer =
            (OBinarySerializer<K>)
                storage
                    .getComponentsFactory()
                    .binarySerializerFactory
                    .getObjectSerializer(rootBucket.getKeySerializerId());
        //noinspection unchecked
        valueSerializer =
            (OBinarySerializer<V>)
                storage
                    .getComponentsFactory()
                    .binarySerializerFactory
                    .getObjectSerializer(rootBucket.getValueSerializerId());

        return !rootBucket.isDeleted();
      }

    } catch (final IOException e) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException("Exception during loading of sbtree " + fileId, this), e);
    } finally {
      lock.unlock();
    }
  }

  public final void load(
      final OBinarySerializer<K> keySerializer, final OBinarySerializer<V> valueSerializer)
      throws IOException {
    final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      this.fileId = openFile(atomicOperation, getFullName());
    } finally {
      lock.unlock();
    }
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry rootCacheEntry =
        loadPageForWrite(atomicOperation, fileId, rootBucketPointer.getPageIndex(), true)) {
      final OSBTreeBonsaiBucket<K, V> rootBucket =
          new OSBTreeBonsaiBucket<>(
              rootCacheEntry,
              rootBucketPointer.getPageOffset(),
              keySerializer,
              valueSerializer,
              this);
      rootBucket.setTreeSize(rootBucket.getTreeSize() + diffSize);
    }
  }

  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        try (final OCacheEntry rootCacheEntry =
            loadPageForRead(atomicOperation, fileId, rootBucketPointer.getPageIndex())) {
          final OSBTreeBonsaiBucket<?, ?> rootBucket =
              new OSBTreeBonsaiBucket<>(
                  rootCacheEntry,
                  rootBucketPointer.getPageOffset(),
                  keySerializer,
                  valueSerializer,
                  this);
          return rootBucket.getTreeSize();
        }
      } finally {
        lock.unlock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during retrieving of size of index " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public V remove(final OAtomicOperation atomicOperation, final K key) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
            if (bucketSearchResult.itemIndex < 0) {
              return null;
            }

            final OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

            final V removed;

            try (final OCacheEntry keyBucketCacheEntry =
                loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), true)) {
              final OSBTreeBonsaiBucket<K, V> keyBucket =
                  new OSBTreeBonsaiBucket<>(
                      keyBucketCacheEntry,
                      bucketPointer.getPageOffset(),
                      keySerializer,
                      valueSerializer,
                      this);

              removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

              keyBucket.remove(bucketSearchResult.itemIndex);
            }
            updateSize(-1, atomicOperation);
            return removed;
          } finally {
            lock.unlock();
          }
        });
  }

  @Override
  public Collection<V> getValuesMinor(
      final K key, final boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<>(64);

    loadEntriesMinor(
        key,
        inclusive,
        entry -> {
          result.add(entry.getValue());
          return maxValuesToFetch <= -1 || result.size() < maxValuesToFetch;
        });

    return result;
  }

  @Override
  public void loadEntriesMinor(
      final K key, final boolean inclusive, final RangeResultListener<K, V> listener) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

        OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();
        int index;
        if (bucketSearchResult.itemIndex >= 0) {
          index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
        } else {
          index = -bucketSearchResult.itemIndex - 2;
        }

        boolean firstBucket = true;
        do {

          try (final OCacheEntry cacheEntry =
              loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex())) {
            final OSBTreeBonsaiBucket<K, V> bucket =
                new OSBTreeBonsaiBucket<>(
                    cacheEntry,
                    bucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);
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
          }
        } while (bucketPointer.getPageIndex() >= 0);
      } finally {
        lock.unlock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during fetch of minor values for key " + key + " in sbtree " + getName(),
              this),
          ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Collection<V> getValuesMajor(
      final K key, final boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<>(64);

    loadEntriesMajor(
        key,
        inclusive,
        true,
        entry -> {
          result.add(entry.getValue());
          return maxValuesToFetch <= -1 || result.size() < maxValuesToFetch;
        });

    return result;
  }

  /**
   * Load all entries with key greater then specified key.
   *
   * @param key defines
   * @param inclusive if true entry with given key is included
   */
  @Override
  public void loadEntriesMajor(
      final K key,
      final boolean inclusive,
      final boolean ascSortOrder,
      final RangeResultListener<K, V> listener) {
    if (!ascSortOrder) {
      throw new IllegalStateException("Descending sort order is not supported.");
    }

    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

        int index;
        if (bucketSearchResult.itemIndex >= 0) {
          index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
        } else {
          index = -bucketSearchResult.itemIndex - 1;
        }

        do {

          try (final OCacheEntry cacheEntry =
              loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex())) {
            final OSBTreeBonsaiBucket<K, V> bucket =
                new OSBTreeBonsaiBucket<>(
                    cacheEntry,
                    bucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);
            final int bucketSize = bucket.size();
            for (int i = index; i < bucketSize; i++) {
              if (!listener.addResult(bucket.getEntry(i))) {
                return;
              }
            }

            bucketPointer = bucket.getRightSibling();
            index = 0;
          }

        } while (bucketPointer.getPageIndex() >= 0);
      } finally {
        lock.unlock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during fetch of major values for key " + key + " in sbtree " + getName(),
              this),
          ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Collection<V> getValuesBetween(
      final K keyFrom,
      final boolean fromInclusive,
      final K keyTo,
      final boolean toInclusive,
      final int maxValuesToFetch) {
    final List<V> result = new ArrayList<>(64);
    loadEntriesBetween(
        keyFrom,
        fromInclusive,
        keyTo,
        toInclusive,
        entry -> {
          result.add(entry.getValue());
          return maxValuesToFetch <= 0 || result.size() < maxValuesToFetch;
        });

    return result;
  }

  private OBonsaiBucketPointer findFirstBucket(
      final OBonsaiBucketPointer root, final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    OBonsaiBucketPointer bucketPointer = root;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, root.getPageIndex());
    int itemIndex = 0;
    try {
      OSBTreeBonsaiBucket<K, V> bucket =
          new OSBTreeBonsaiBucket<>(
              cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);

      while (true) {
        if (bucket.isLeaf()) {
          if (bucket.isEmpty()) {
            if (path.isEmpty()) {
              return null;
            } else {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketPointer = pagePathItemUnit.bucketPointer;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            }
          } else {
            return new OBonsaiBucketPointer(
                cacheEntry.getPageIndex(), bucketPointer.getPageOffset());
          }
        } else {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (path.isEmpty()) {
              return null;
            } else {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketPointer = pagePathItemUnit.bucketPointer;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            }
          } else {
            path.add(new PagePathItemUnit(bucketPointer, itemIndex));

            if (itemIndex < bucket.size()) {
              final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
              bucketPointer = entry.leftChild;
            } else {
              final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex - 1);
              bucketPointer = entry.rightChild;
            }

            itemIndex = 0;
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex());

        bucket =
            new OSBTreeBonsaiBucket<>(
                cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
      }
    } finally {
      cacheEntry.close();
    }
  }

  @Override
  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final OBonsaiBucketPointer firstBucket =
            findFirstBucket(this.rootBucketPointer, atomicOperation);
        if (firstBucket == null) {
          return null;
        }

        try (final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, firstBucket.getPageIndex())) {
          final OSBTreeBonsaiBucket<K, V> bucket =
              new OSBTreeBonsaiBucket<>(
                  cacheEntry, firstBucket.getPageOffset(), keySerializer, valueSerializer, this);
          return bucket.getKey(0);
        }
      } finally {
        lock.unlock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during finding first key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final LinkedList<PagePathItemUnit> path = new LinkedList<>();

        OBonsaiBucketPointer bucketPointer = rootBucketPointer;

        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex());
        OSBTreeBonsaiBucket<K, V> bucket =
            new OSBTreeBonsaiBucket<>(
                cacheEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);

        int itemIndex = bucket.size() - 1;
        try {
          while (true) {
            if (bucket.isLeaf()) {
              if (bucket.isEmpty()) {
                if (path.isEmpty()) {
                  return null;
                } else {
                  final PagePathItemUnit pagePathItemUnit = path.removeLast();

                  bucketPointer = pagePathItemUnit.bucketPointer;
                  itemIndex = pagePathItemUnit.itemIndex - 1;
                }
              } else {
                return bucket.getKey(bucket.size() - 1);
              }
            } else {
              if (itemIndex < -1) {
                if (!path.isEmpty()) {
                  final PagePathItemUnit pagePathItemUnit = path.removeLast();

                  bucketPointer = pagePathItemUnit.bucketPointer;
                  itemIndex = pagePathItemUnit.itemIndex - 1;
                } else {
                  return null;
                }
              } else {
                path.add(new PagePathItemUnit(bucketPointer, itemIndex));

                if (itemIndex > -1) {
                  final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
                  bucketPointer = entry.rightChild;
                } else {
                  final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(0);
                  bucketPointer = entry.leftChild;
                }

                itemIndex = OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1;
              }
            }

            cacheEntry.close();

            cacheEntry = loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex());

            bucket =
                new OSBTreeBonsaiBucket<>(
                    cacheEntry,
                    bucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);
            if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1) {
              itemIndex = bucket.size() - 1;
            }
          }
        } finally {
          cacheEntry.close();
        }
      } finally {
        lock.unlock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during finding first key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void loadEntriesBetween(
      final K keyFrom,
      final boolean fromInclusive,
      final K keyTo,
      final boolean toInclusive,
      final RangeResultListener<K, V> listener) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      final Lock lock = FILE_LOCK_MANAGER.acquireSharedLock(fileId);
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom, atomicOperation);

        final OBonsaiBucketPointer bucketPointerFrom = bucketSearchResultFrom.getLastPathItem();

        final int indexFrom;
        if (bucketSearchResultFrom.itemIndex >= 0) {
          indexFrom =
              fromInclusive
                  ? bucketSearchResultFrom.itemIndex
                  : bucketSearchResultFrom.itemIndex + 1;
        } else {
          indexFrom = -bucketSearchResultFrom.itemIndex - 1;
        }

        final BucketSearchResult bucketSearchResultTo = findBucket(keyTo, atomicOperation);
        final OBonsaiBucketPointer bucketPointerTo = bucketSearchResultTo.getLastPathItem();

        final int indexTo;
        if (bucketSearchResultTo.itemIndex >= 0) {
          indexTo =
              toInclusive ? bucketSearchResultTo.itemIndex : bucketSearchResultTo.itemIndex - 1;
        } else {
          indexTo = -bucketSearchResultTo.itemIndex - 2;
        }

        int startIndex = indexFrom;
        int endIndex;
        OBonsaiBucketPointer bucketPointer = bucketPointerFrom;

        resultsLoop:
        while (true) {

          try (final OCacheEntry cacheEntry =
              loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex())) {
            final OSBTreeBonsaiBucket<K, V> bucket =
                new OSBTreeBonsaiBucket<>(
                    cacheEntry,
                    bucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);
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
          }

          startIndex = 0;
        }
      } finally {
        lock.unlock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OSBTreeBonsaiLocalException(
              "Error during fetch of values between key "
                  + keyFrom
                  + " and key "
                  + keyTo
                  + " in sbtree "
                  + getName(),
              this),
          ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void flush() {
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
  }

  private BucketSearchResult splitBucket(
      final List<OBonsaiBucketPointer> path,
      final int keyIndex,
      final K keyToInsert,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final OBonsaiBucketPointer bucketPointer = path.get(path.size() - 1);

    try (final OCacheEntry bucketEntry =
        loadPageForWrite(atomicOperation, fileId, bucketPointer.getPageIndex(), true)) {
      OSBTreeBonsaiBucket<K, V> bucketToSplit =
          new OSBTreeBonsaiBucket<>(
              bucketEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      final int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> rightEntries =
          new ArrayList<>(indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (!bucketPointer.equals(rootBucketPointer)) {
        final AllocationResult allocationResult =
            allocateBucketForWrite(
                atomicOperation, -1, -1, atomicOperation.getDeletedBonsaiPointers());
        final OBonsaiBucketPointer rightBucketPointer = allocationResult.getPointer();

        try (final OCacheEntry rightBucketEntry = allocationResult.getCacheEntry()) {
          final OSBTreeBonsaiBucket<K, V> newRightBucket =
              new OSBTreeBonsaiBucket<>(
                  rightBucketEntry,
                  rightBucketPointer.getPageOffset(),
                  splitLeaf,
                  keySerializer,
                  valueSerializer,
                  this);
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            final OBonsaiBucketPointer rightSiblingBucketPointer = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingBucketPointer);
            newRightBucket.setLeftSibling(bucketPointer);

            bucketToSplit.setRightSibling(rightBucketPointer);

            if (rightSiblingBucketPointer.isValid()) {

              try (final OCacheEntry rightSiblingBucketEntry =
                  loadPageForWrite(
                      atomicOperation, fileId, rightSiblingBucketPointer.getPageIndex(), true); ) {
                final OSBTreeBonsaiBucket<K, V> rightSiblingBucket =
                    new OSBTreeBonsaiBucket<>(
                        rightSiblingBucketEntry,
                        rightSiblingBucketPointer.getPageOffset(),
                        keySerializer,
                        valueSerializer,
                        this);
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry =
              loadPageForWrite(atomicOperation, fileId, parentBucketPointer.getPageIndex(), true);

          try {
            OSBTreeBonsaiBucket<K, V> parentBucket =
                new OSBTreeBonsaiBucket<>(
                    parentCacheEntry,
                    parentBucketPointer.getPageOffset(),
                    keySerializer,
                    valueSerializer,
                    this);
            final OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry =
                new OSBTreeBonsaiBucket.SBTreeEntry<>(
                    bucketPointer, rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentCacheEntry.close();

              final BucketSearchResult bucketSearchResult =
                  splitBucket(
                      path.subList(0, path.size() - 1),
                      insertionIndex,
                      separationKey,
                      atomicOperation);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry =
                  loadPageForWrite(
                      atomicOperation, fileId, parentBucketPointer.getPageIndex(), true);

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket =
                  new OSBTreeBonsaiBucket<>(
                      parentCacheEntry,
                      parentBucketPointer.getPageOffset(),
                      keySerializer,
                      valueSerializer,
                      this);
            }

          } finally {
            parentCacheEntry.close();
          }
        }

        final ArrayList<OBonsaiBucketPointer> resultPath =
            new ArrayList<>(path.subList(0, path.size() - 1));

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
        final long treeSize = bucketToSplit.getTreeSize();

        final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> leftEntries =
            new ArrayList<>(indexToSplit);

        for (int i = 0; i < indexToSplit; i++) leftEntries.add(bucketToSplit.getEntry(i));

        final AllocationResult leftAllocationResult =
            allocateBucketForWrite(
                atomicOperation, -1, -1, atomicOperation.getDeletedBonsaiPointers());

        final OBonsaiBucketPointer leftBucketPointer = leftAllocationResult.getPointer();

        final AllocationResult rightAllocationResult =
            allocateBucketForWrite(
                atomicOperation, -1, -1, atomicOperation.getDeletedBonsaiPointers());

        final OBonsaiBucketPointer rightBucketPointer = rightAllocationResult.getPointer();
        try (final OCacheEntry leftBucketEntry = leftAllocationResult.getCacheEntry()) {
          final OSBTreeBonsaiBucket<K, V> newLeftBucket =
              new OSBTreeBonsaiBucket<>(
                  leftBucketEntry,
                  leftBucketPointer.getPageOffset(),
                  splitLeaf,
                  keySerializer,
                  valueSerializer,
                  this);
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf) {
            newLeftBucket.setRightSibling(rightBucketPointer);
          }
        }

        try (final OCacheEntry rightBucketEntry = rightAllocationResult.getCacheEntry()) {
          final OSBTreeBonsaiBucket<K, V> newRightBucket =
              new OSBTreeBonsaiBucket<>(
                  rightBucketEntry,
                  rightBucketPointer.getPageOffset(),
                  splitLeaf,
                  keySerializer,
                  valueSerializer,
                  this);
          newRightBucket.addAll(rightEntries);

          if (splitLeaf) {
            newRightBucket.setLeftSibling(leftBucketPointer);
          }
        }

        bucketToSplit =
            new OSBTreeBonsaiBucket<>(
                bucketEntry,
                bucketPointer.getPageOffset(),
                false,
                keySerializer,
                valueSerializer,
                this);
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit.addEntry(
            0,
            new OSBTreeBonsaiBucket.SBTreeEntry<>(
                leftBucketPointer, rightBucketPointer, separationKey, null),
            true);

        final ArrayList<OBonsaiBucketPointer> resultPath =
            new ArrayList<>(path.subList(0, path.size() - 1));

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
    }
  }

  private BucketSearchResult findBucket(final K key, final OAtomicOperation atomicOperation)
      throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<>(8);

    while (true) {
      path.add(bucketPointer);

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try (final OCacheEntry bucketEntry =
          loadPageForRead(atomicOperation, fileId, bucketPointer.getPageIndex())) {
        final OSBTreeBonsaiBucket<K, V> keyBucket =
            new OSBTreeBonsaiBucket<>(
                bucketEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
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
      }

      if (comparator.compare(key, entry.key) >= 0) {
        bucketPointer = entry.rightChild;
      } else {
        bucketPointer = entry.leftChild;
      }
    }
  }

  private void initSysBucket(final OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry sysCacheEntry =
        loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), true);
    if (sysCacheEntry == null) {
      sysCacheEntry = addPage(atomicOperation, fileId);
      assert sysCacheEntry.getPageIndex() == SYS_BUCKET.getPageIndex();
    }

    try {
      OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
      if (sysBucket.isNotInitialized()) {
        sysBucket.init();
      }
    } finally {
      sysCacheEntry.close();
    }
  }

  private AllocationResult allocateBucketForWrite(
      final OAtomicOperation atomicOperation,
      final int requestedPageIndex,
      final int requestedPageOffset,
      Set<OBonsaiBucketPointer> blockedPointers)
      throws IOException {

    try (final OCacheEntry sysCacheEntry =
        loadPageForWrite(atomicOperation, fileId, SYS_BUCKET.getPageIndex(), true)) {

      if (requestedPageIndex == -1) {
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
        if (sysBucket.freeListLength() > 0) {
          final AllocationResult allocationResult =
              reuseBucketFromFreeList(atomicOperation, sysBucket, blockedPointers, -1, -1);
          if (allocationResult != null) {
            return allocationResult;
          }
        }
        return allocateNewPage(atomicOperation, sysBucket);
      } else {
        // during rollback or data restore we need to restore ridbag with exact value of
        final OSysBucket sysBucket = new OSysBucket(sysCacheEntry);
        AllocationResult allocationResult = null;

        if (sysBucket.freeListLength() > 0) {
          allocationResult =
              reuseBucketFromFreeList(
                  atomicOperation,
                  sysBucket,
                  blockedPointers,
                  requestedPageIndex,
                  requestedPageOffset);
        }

        if (allocationResult == null) {
          allocationResult = allocateNewPage(atomicOperation, sysBucket);
        }

        if (allocationResult.pointer.getPageIndex() != requestedPageIndex
            || allocationResult.pointer.getPageOffset() != requestedPageOffset) {
          allocationResult.cacheEntry.close();

          throw new OStorageException(
              "Can not allocate rid bag with pageIndex = "
                  + requestedPageIndex
                  + ", pageOffset = "
                  + requestedPageOffset);
        }

        return allocationResult;
      }
    }
  }

  private AllocationResult allocateNewPage(
      final OAtomicOperation atomicOperation, final OSysBucket sysBucket) throws IOException {
    final OBonsaiBucketPointer freeSpacePointer = sysBucket.getFreeSpacePointer();
    if (freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES > PAGE_SIZE) {
      final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
      final long pageIndex = cacheEntry.getPageIndex();
      sysBucket.setFreeSpacePointer(
          new OBonsaiBucketPointer(pageIndex, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));

      return new AllocationResult(new OBonsaiBucketPointer(pageIndex, 0), cacheEntry);
    } else {
      sysBucket.setFreeSpacePointer(
          new OBonsaiBucketPointer(
              freeSpacePointer.getPageIndex(),
              freeSpacePointer.getPageOffset() + OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
      final OCacheEntry cacheEntry =
          loadPageForWrite(atomicOperation, fileId, freeSpacePointer.getPageIndex(), true);

      return new AllocationResult(freeSpacePointer, cacheEntry);
    }
  }

  private AllocationResult reuseBucketFromFreeList(
      final OAtomicOperation atomicOperation,
      final OSysBucket sysBucket,
      Set<OBonsaiBucketPointer> blockedPointers,
      final int requestedPageIndex,
      final int requestedPageOffset)
      throws IOException {
    OBonsaiBucketPointer freeListItem = sysBucket.getFreeListHead();
    final OBonsaiBucketPointer freeListHead = freeListItem;

    assert freeListItem.isValid();

    OSBTreeBonsaiBucket<K, V> bucket;
    OCacheEntry cacheEntry;

    long prevPageIndex = -1;
    int prevPageOffset = -1;

    while (true) {
      cacheEntry = loadPageForWrite(atomicOperation, fileId, freeListItem.getPageIndex(), true);
      bucket =
          new OSBTreeBonsaiBucket<>(
              cacheEntry, freeListItem.getPageOffset(), keySerializer, valueSerializer, this);

      // current item blocked or we want item with specific page index and page offset but did not
      // find it yet
      if (blockedPointers.contains(freeListItem)
          && !(requestedPageIndex == -1
              || (freeListItem.getPageIndex() == requestedPageIndex
                  && freeListItem.getPageOffset() == requestedPageOffset))) {
        freeListItem = bucket.getFreeListPointer();
        cacheEntry.close();

        prevPageIndex = freeListItem.getPageIndex();
        prevPageOffset = freeListItem.getPageOffset();

        // location of sys bucket and default value
        if (freeListItem.getPageOffset() == 0 && freeListItem.getPageIndex() == 0) {
          return null;
        }
      } else {
        break;
      }
    }

    if (freeListHead.equals(freeListItem)) {
      sysBucket.setFreeListHead(bucket.getFreeListPointer());
    } else {
      assert prevPageIndex >= 0;
      assert prevPageOffset >= 0;

      if (prevPageIndex == cacheEntry.getPageIndex()) {
        final OSBTreeBonsaiBucket<K, V> prevBucket =
            new OSBTreeBonsaiBucket<>(
                cacheEntry, prevPageOffset, keySerializer, valueSerializer, this);
        prevBucket.setFreeListPointer(bucket.getFreeListPointer());
      } else {
        try (final OCacheEntry prevCacheEntry =
            loadPageForWrite(atomicOperation, fileId, prevPageIndex, true)) {
          final OSBTreeBonsaiBucket<K, V> prevBucket =
              new OSBTreeBonsaiBucket<>(
                  prevCacheEntry, prevPageOffset, keySerializer, valueSerializer, this);
          prevBucket.setFreeListPointer(bucket.getFreeListPointer());
        }
      }
    }

    sysBucket.setFreeListLength(sysBucket.freeListLength() - 1);

    return new AllocationResult(freeListItem, cacheEntry);
  }

  @Override
  public int getRealBagSize(final Map<K, Change> changes) {
    final Map<K, Change> notAppliedChanges = new HashMap<>(changes);
    final OModifiableInteger size = new OModifiableInteger(0);
    loadEntriesMajor(
        firstKey(),
        true,
        true,
        entry -> {
          final Change change = notAppliedChanges.remove(entry.getKey());
          final int result;

          final Integer treeValue = (Integer) entry.getValue();
          if (change == null) {
            result = treeValue;
          } else {
            result = change.applyTo(treeValue);
          }

          size.increment(result);
          return true;
        });

    for (final Change change : notAppliedChanges.values()) {
      size.increment(change.applyTo(0));
    }

    return size.intValue();
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
    private final OCacheEntry cacheEntry;

    private AllocationResult(final OBonsaiBucketPointer pointer, final OCacheEntry cacheEntry) {
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
    private final int itemIndex;
    private final ArrayList<OBonsaiBucketPointer> path;

    private BucketSearchResult(final int itemIndex, final ArrayList<OBonsaiBucketPointer> path) {
      this.itemIndex = itemIndex;
      this.path = path;
    }

    private OBonsaiBucketPointer getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {
    private final OBonsaiBucketPointer bucketPointer;
    private final int itemIndex;

    private PagePathItemUnit(final OBonsaiBucketPointer bucketPointer, final int itemIndex) {
      this.bucketPointer = bucketPointer;
      this.itemIndex = itemIndex;
    }
  }

  public void debugPrintBucket(final PrintStream writer) throws IOException {
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<>(8);
    path.add(rootBucketPointer);
    debugPrintBucket(rootBucketPointer, writer, path);
  }

  @SuppressWarnings({"resource", "StringConcatenationInsideStringBufferAppend"})
  private void debugPrintBucket(
      final OBonsaiBucketPointer bucketPointer,
      final PrintStream writer,
      final ArrayList<OBonsaiBucketPointer> path)
      throws IOException {

    OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
    try (final OCacheEntry bucketEntry =
        loadPageForRead(null, fileId, bucketPointer.getPageIndex())) {
      final OSBTreeBonsaiBucket<K, V> keyBucket =
          new OSBTreeBonsaiBucket<>(
              bucketEntry, bucketPointer.getPageOffset(), keySerializer, valueSerializer, this);
      if (keyBucket.isLeaf()) {
        for (int i = 0; i < path.size(); i++) {
          writer.append("\t");
        }
        writer.append(
            " Leaf backet:" + bucketPointer.getPageIndex() + "|" + bucketPointer.getPageOffset());
        writer.append(
            " left bucket:"
                + keyBucket.getLeftSibling().getPageIndex()
                + "|"
                + keyBucket.getLeftSibling().getPageOffset());
        writer.append(
            " right bucket:"
                + keyBucket.getRightSibling().getPageIndex()
                + "|"
                + keyBucket.getRightSibling().getPageOffset());
        writer.append(" size:" + keyBucket.size());
        writer.append(" content: [");
        for (int index = 0; index < keyBucket.size(); index++) {
          entry = keyBucket.getEntry(index);
          writer.append(entry.getKey() + ",");
        }
        writer.append("\n");
      } else {
        for (int i = 0; i < path.size(); i++) {
          writer.append("\t");
        }
        writer.append(
            " node bucket:" + bucketPointer.getPageIndex() + "|" + bucketPointer.getPageOffset());
        writer.append(
            " left bucket:"
                + keyBucket.getLeftSibling().getPageIndex()
                + "|"
                + keyBucket.getLeftSibling().getPageOffset());
        writer.append(
            " right bucket:"
                + keyBucket.getRightSibling().getPageIndex()
                + "|"
                + keyBucket.getRightSibling().getPageOffset());
        writer.append("\n");
        for (int index = 0; index < keyBucket.size(); index++) {
          entry = keyBucket.getEntry(index);
          for (int i = 0; i < path.size(); i++) {
            writer.append("\t");
          }
          writer.append(" entry:" + index + " key: " + entry.getKey() + " left \n");
          OBonsaiBucketPointer next = entry.leftChild;
          path.add(next);
          debugPrintBucket(next, writer, path);
          path.remove(next);
          for (int i = 0; i < path.size(); i++) {
            writer.append("\t");
          }
          writer.append(" entry:" + index + " key: " + entry.getKey() + " right \n");
          next = entry.rightChild;
          path.add(next);
          debugPrintBucket(next, writer, path);
          path.remove(next);
        }
      }
    }
  }

  public void markToDelete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          final Lock lock = FILE_LOCK_MANAGER.acquireExclusiveLock(fileId);
          try {
            try (final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, rootBucketPointer.getPageIndex(), true)) {
              OSBTreeBonsaiBucket<K, V> rootBucket =
                  new OSBTreeBonsaiBucket<>(
                      cacheEntry,
                      rootBucketPointer.getPageOffset(),
                      keySerializer,
                      valueSerializer,
                      this);
              rootBucket.setToDelete(true);
            }
          } finally {
            lock.unlock();
          }
        });
  }
}
