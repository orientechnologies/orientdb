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

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This is implementation which is based on B+-tree implementation threaded tree. The main
 * differences are:
 *
 * <ol>
 *   <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused
 *       later when new items are added.
 *   <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more
 *       than keys contained in current bucket
 *       <ol/>
 *         There is support of null values for keys, but values itself cannot be null. Null keys
 *         support is switched off by default if null keys are supported value which is related to
 *         null key will be stored in separate file which has only one page. Buckets/pages for usual
 *         (non-null) key-value entries can be considered as sorted array. The first bytes of page
 *         contains such auxiliary information as size of entries contained in bucket, links to
 *         neighbors which contain entries with keys less/more than keys in current bucket. The next
 *         bytes contain sorted array of entries. Array itself is split on two parts. First part is
 *         growing from start to end, and second part is growing from end to start. First part is
 *         array of offsets to real key-value entries which are stored in second part of array which
 *         grows from end to start. This array of offsets is sorted by accessing order according to
 *         key value. So we can use binary search to find requested key. When new key-value pair is
 *         added we append binary presentation of this pair to the second part of array which grows
 *         from end of page to start, remember value of offset for this pair, and find proper
 *         position of this offset inside of first part of array. Such approach allows to minimize
 *         amount of memory involved in performing of operations and as result speed up data
 *         processing.
 *
 * @author Andrey Lomakin (lomakin.andrey-at-gmail.com)
 * @since 8/7/13
 */
public final class CellBTreeSingleValueV3<K> extends ODurableComponent
    implements OCellBTreeSingleValue<K> {
  private static final int SPLITERATOR_CACHE_SIZE =
      OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
  private static final int MAX_KEY_SIZE =
      OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final OAlwaysLessKey ALWAYS_LESS_KEY = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH =
      OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final int ENTRY_POINT_INDEX = 0;
  private static final long ROOT_INDEX = 1;
  final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final String nullFileExtension;
  private long fileId;
  private long nullBucketFileId = -1;
  private int keySize;
  private OBinarySerializer<K> keySerializer;
  private OType[] keyTypes;

  public CellBTreeSingleValueV3(
      final String name,
      final String dataFileExtension,
      final String nullFileExtension,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(
      final OAtomicOperation atomicOperation,
      final OBinarySerializer<K> keySerializer,
      final OType[] keyTypes,
      final int keySize,
      final OEncryption encryption) {
    assert keySerializer != null;

    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            this.keySize = keySize;
            if (keyTypes != null) {
              this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
            } else {
              this.keyTypes = null;
            }
            this.keySerializer = keySerializer;

            fileId = addFile(atomicOperation, getFullName());
            nullBucketFileId = addFile(atomicOperation, getName() + nullFileExtension);

            try (final OCacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId)) {
              final CellBTreeSingleValueEntryPointV3<K> entryPoint =
                  new CellBTreeSingleValueEntryPointV3<>(entryPointCacheEntry);
              entryPoint.init();
            }

            try (final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId)) {
              @SuppressWarnings("unused")
              final CellBTreeSingleValueBucketV3<K> rootBucket =
                  new CellBTreeSingleValueBucketV3<>(rootCacheEntry);
              rootBucket.init(true);
            }

            try (final OCacheEntry nullCacheEntry = addPage(atomicOperation, nullBucketFileId)) {
              @SuppressWarnings("unused")
              final CellBTreeSingleValueV3NullBucket nullBucket =
                  new CellBTreeSingleValueV3NullBucket(nullCacheEntry);
              nullBucket.init();
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public ORID get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.getItemIndex() < 0) {
            return null;
          }

          final long pageIndex = bucketSearchResult.getPageIndex();

          try (final OCacheEntry keyBucketCacheEntry =
              loadPageForRead(atomicOperation, fileId, pageIndex)) {
            final CellBTreeSingleValueBucketV3<K> keyBucket =
                new CellBTreeSingleValueBucketV3<>(keyBucketCacheEntry);
            return keyBucket.getValue(bucketSearchResult.getItemIndex(), keySerializer);
          }
        } else {

          try (final OCacheEntry nullBucketCacheEntry =
              loadPageForRead(atomicOperation, nullBucketFileId, 0); ) {
            final CellBTreeSingleValueV3NullBucket nullBucket =
                new CellBTreeSingleValueV3NullBucket(nullBucketCacheEntry);
            return nullBucket.getValue();
          }
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during retrieving  of sbtree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void put(final OAtomicOperation atomicOperation, final K key, final ORID value) {
    update(atomicOperation, key, value, null);
  }

  public boolean validatedPut(
      OAtomicOperation atomicOperation,
      final K key,
      final ORID value,
      final OBaseIndexEngine.Validator<K, ORID> validator) {
    return update(atomicOperation, key, value, validator);
  }

  private boolean update(
      final OAtomicOperation atomicOperation,
      final K k,
      final ORID rid,
      final OBaseIndexEngine.Validator<K, ORID> validator) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            K key = k;
            ORID value = rid;

            if (key != null) {
              key = keySerializer.preprocess(key, (Object[]) keyTypes);
              final byte[] serializedKey =
                  keySerializer.serializeNativeAsWhole(key, (Object[]) keyTypes);
              if (serializedKey.length > MAX_KEY_SIZE) {
                throw new OTooBigIndexKeyException(
                    "Key size is more than allowed, operation was canceled. Current key size "
                        + serializedKey.length
                        + ", allowed  "
                        + MAX_KEY_SIZE,
                    getName());
              }
              UpdateBucketSearchResult bucketSearchResult =
                  findBucketForUpdate(key, atomicOperation);

              OCacheEntry keyBucketCacheEntry =
                  loadPageForWrite(
                      atomicOperation, fileId, bucketSearchResult.getLastPathItem(), true);
              CellBTreeSingleValueBucketV3<K> keyBucket =
                  new CellBTreeSingleValueBucketV3<>(keyBucketCacheEntry);
              final byte[] oldRawValue;
              if (bucketSearchResult.getItemIndex() > -1) {
                oldRawValue =
                    keyBucket.getRawValue(bucketSearchResult.getItemIndex(), keySerializer);
              } else {
                oldRawValue = null;
              }
              final ORID oldValue;
              if (oldRawValue == null) {
                oldValue = null;
              } else {
                final int clusterId = OShortSerializer.INSTANCE.deserializeNative(oldRawValue, 0);
                final long clusterPosition =
                    OLongSerializer.INSTANCE.deserializeNative(
                        oldRawValue, OShortSerializer.SHORT_SIZE);
                oldValue = new ORecordId(clusterId, clusterPosition);
              }

              if (validator != null) {
                boolean failure = true; // assuming validation throws by default
                boolean ignored = false;

                try {

                  final Object result = validator.validate(key, oldValue, value);
                  if (result == OBaseIndexEngine.Validator.IGNORE) {
                    ignored = true;
                    failure = false;
                    return false;
                  }

                  value = (ORID) result;
                  failure = false;
                } finally {
                  if (failure || ignored) {
                    keyBucketCacheEntry.close();
                  }
                }
              }

              final byte[] serializedValue =
                  new byte[OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE];
              OShortSerializer.INSTANCE.serializeNative(
                  (short) value.getClusterId(), serializedValue, 0);
              OLongSerializer.INSTANCE.serializeNative(
                  value.getClusterPosition(), serializedValue, OShortSerializer.SHORT_SIZE);

              int insertionIndex;
              final int sizeDiff;
              if (bucketSearchResult.getItemIndex() >= 0) {
                assert oldRawValue != null;

                if (oldRawValue.length == serializedValue.length) {
                  keyBucket.updateValue(
                      bucketSearchResult.getItemIndex(), serializedValue, serializedKey.length);
                  keyBucketCacheEntry.close();
                  return true;
                } else {
                  keyBucket.removeLeafEntry(bucketSearchResult.getItemIndex(), serializedKey);
                  insertionIndex = bucketSearchResult.getItemIndex();
                  sizeDiff = 0;
                }
              } else {
                insertionIndex = -bucketSearchResult.getItemIndex() - 1;
                sizeDiff = 1;
              }

              while (!keyBucket.addLeafEntry(insertionIndex, serializedKey, serializedValue)) {
                bucketSearchResult =
                    splitBucket(
                        keyBucket,
                        keyBucketCacheEntry,
                        bucketSearchResult.getPath(),
                        bucketSearchResult.getInsertionIndexes(),
                        insertionIndex,
                        atomicOperation);

                insertionIndex = bucketSearchResult.getItemIndex();

                final long pageIndex = bucketSearchResult.getLastPathItem();

                if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
                  keyBucketCacheEntry.close();

                  keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
                }

                //noinspection ObjectAllocationInLoop
                keyBucket = new CellBTreeSingleValueBucketV3<>(keyBucketCacheEntry);
              }

              keyBucketCacheEntry.close();

              if (sizeDiff != 0) {
                updateSize(sizeDiff, atomicOperation);
              }
            } else {
              int sizeDiff = 0;
              final ORID oldValue;
              try (final OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, nullBucketFileId, 0, true)) {
                final CellBTreeSingleValueV3NullBucket nullBucket =
                    new CellBTreeSingleValueV3NullBucket(cacheEntry);
                oldValue = nullBucket.getValue();

                if (validator != null) {
                  final Object result = validator.validate(null, oldValue, value);
                  if (result == OBaseIndexEngine.Validator.IGNORE) {
                    return false;
                  }
                }

                if (oldValue != null) {
                  sizeDiff = -1;
                }
                nullBucket.setValue(value);
              }
              sizeDiff++;
              updateSize(sizeDiff, atomicOperation);
            }
            return true;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void close() {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, true, writeCache);
      readCache.closeFile(nullBucketFileId, true, writeCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final long size = size();
            if (size > 0) {
              throw new NotEmptyComponentCanNotBeRemovedException(
                  getName()
                      + " : Not empty index can not be deleted. Index has "
                      + size
                      + " records");
            }

            deleteFile(atomicOperation, fileId);
            deleteFile(atomicOperation, nullBucketFileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void load(
      final String name,
      final int keySize,
      final OType[] keyTypes,
      final OBinarySerializer<K> keySerializer,
      final OEncryption encryption) {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
      nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);

      this.keySize = keySize;
      this.keyTypes = keyTypes;
      this.keySerializer = keySerializer;
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception("Exception during loading of sbtree " + name, this),
          e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        try (final OCacheEntry entryPointCacheEntry =
            loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX)) {
          final CellBTreeSingleValueEntryPointV3<K> entryPoint =
              new CellBTreeSingleValueEntryPointV3<>(entryPointCacheEntry);
          return entryPoint.getTreeSize();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during retrieving of size of index " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public ORID remove(final OAtomicOperation atomicOperation, final K key) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            if (key != null) {
              final ORID removedValue;

              final Optional<RemoveSearchResult> bucketSearchResult =
                  findBucketForRemove(key, atomicOperation);
              if (bucketSearchResult.isPresent()) {
                final RemoveSearchResult removeSearchResult = bucketSearchResult.get();

                final byte[] rawValue;
                final int bucketSize;
                try (final OCacheEntry keyBucketCacheEntry =
                    loadPageForWrite(
                        atomicOperation, fileId, removeSearchResult.getLeafPageIndex(), true)) {
                  final CellBTreeSingleValueBucketV3<K> keyBucket =
                      new CellBTreeSingleValueBucketV3<>(keyBucketCacheEntry);
                  rawValue =
                      keyBucket.getRawValue(
                          removeSearchResult.getLeafEntryPageIndex(), keySerializer);
                  final byte[] serializedKey =
                      keyBucket.getRawKey(
                          removeSearchResult.getLeafEntryPageIndex(), keySerializer);
                  bucketSize =
                      keyBucket.removeLeafEntry(
                          removeSearchResult.getLeafEntryPageIndex(), serializedKey);
                  updateSize(-1, atomicOperation);

                  final int clusterId = OShortSerializer.INSTANCE.deserializeNative(rawValue, 0);
                  final long clusterPosition =
                      OLongSerializer.INSTANCE.deserializeNative(
                          rawValue, OShortSerializer.SHORT_SIZE);

                  removedValue = new ORecordId(clusterId, clusterPosition);

                  // skip balancing of the tree if leaf is a root.
                  if (bucketSize == 0 && removeSearchResult.getPath().size() > 0) {
                    final boolean balanceResult =
                        balanceLeafNodeAfterItemDelete(
                            atomicOperation, removeSearchResult, keyBucket);

                    if (balanceResult) {
                      addToFreeList(atomicOperation, (int) removeSearchResult.getLeafPageIndex());
                    }
                  }
                }
              } else {
                return null;
              }

              return removedValue;
            } else {
              return removeNullBucket(atomicOperation);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private boolean balanceLeafNodeAfterItemDelete(
      final OAtomicOperation atomicOperation,
      final RemoveSearchResult removeSearchResult,
      final CellBTreeSingleValueBucketV3<K> keyBucket)
      throws IOException {
    final RemovalPathItem parentItem =
        removeSearchResult.getPath().get(removeSearchResult.getPath().size() - 1);

    try (final OCacheEntry parentCacheEntry =
        loadPageForWrite(atomicOperation, fileId, parentItem.getPageIndex(), true)) {
      final CellBTreeSingleValueBucketV3<K> parentBucket =
          new CellBTreeSingleValueBucketV3<>(parentCacheEntry);

      if (parentItem.isLeftChild()) {
        final int rightSiblingPageIndex = parentBucket.getRight(parentItem.getIndexInsidePage());

        // merge with left sibling
        try (final OCacheEntry rightSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true)) {
          final CellBTreeSingleValueBucketV3<K> rightSiblingBucket =
              new CellBTreeSingleValueBucketV3<>(rightSiblingEntry);
          final boolean success =
              deleteFromNonLeafNode(atomicOperation, parentBucket, removeSearchResult.getPath());

          if (success) {
            final long leftSiblingIndex = keyBucket.getLeftSibling();
            assert rightSiblingBucket.getLeftSibling() == keyBucket.getCacheEntry().getPageIndex();

            rightSiblingBucket.setLeftSibling(leftSiblingIndex);

            if (leftSiblingIndex > 0) {
              try (final OCacheEntry leftSiblingEntry =
                  loadPageForWrite(atomicOperation, fileId, leftSiblingIndex, true)) {
                final CellBTreeSingleValueBucketV3<K> leftSiblingBucket =
                    new CellBTreeSingleValueBucketV3<>(leftSiblingEntry);

                leftSiblingBucket.setRightSibling(rightSiblingPageIndex);
              }
            }

            return true;
          }

          return false;
        }
      }

      final int leftSiblingPageIndex = parentBucket.getLeft(parentItem.getIndexInsidePage());
      try (final OCacheEntry leftSiblingEntry =
          loadPageForWrite(atomicOperation, fileId, leftSiblingPageIndex, true)) {
        // merge with right sibling
        final CellBTreeSingleValueBucketV3<K> leftSiblingBucket =
            new CellBTreeSingleValueBucketV3<>(leftSiblingEntry);
        final boolean success =
            deleteFromNonLeafNode(atomicOperation, parentBucket, removeSearchResult.getPath());

        if (success) {
          final long rightSiblingIndex = keyBucket.getRightSibling();

          assert leftSiblingBucket.getRightSibling() == keyBucket.getCacheEntry().getPageIndex();
          leftSiblingBucket.setRightSibling(rightSiblingIndex);

          if (rightSiblingIndex > 0) {
            try (final OCacheEntry rightSiblingEntry =
                loadPageForWrite(atomicOperation, fileId, rightSiblingIndex, true)) {
              final CellBTreeSingleValueBucketV3<K> rightSibling =
                  new CellBTreeSingleValueBucketV3<>(rightSiblingEntry);
              rightSibling.setLeftSibling(leftSiblingPageIndex);
            }
          }

          return true;
        }

        return false;
      }
    }
  }

  private boolean deleteFromNonLeafNode(
      final OAtomicOperation atomicOperation,
      final CellBTreeSingleValueBucketV3<K> bucket,
      final List<RemovalPathItem> path)
      throws IOException {
    final int bucketSize = bucket.size();
    assert bucketSize > 0;

    // currently processed node is a root node
    if (path.size() == 1) {
      if (bucketSize == 1) {
        return false;
      }
    }

    final RemovalPathItem currentItem = path.get(path.size() - 1);

    if (bucketSize > 1) {
      bucket.removeNonLeafEntry(
          currentItem.getIndexInsidePage(), currentItem.isLeftChild(), keySerializer);

      return true;
    }

    final RemovalPathItem parentItem = path.get(path.size() - 2);

    try (final OCacheEntry parentCacheEntry =
        loadPageForWrite(atomicOperation, fileId, parentItem.getPageIndex(), true)) {
      final CellBTreeSingleValueBucketV3<K> parentBucket =
          new CellBTreeSingleValueBucketV3<>(parentCacheEntry);

      final int orphanPointer;
      if (currentItem.isLeftChild()) {
        orphanPointer = bucket.getRight(currentItem.getIndexInsidePage());
      } else {
        orphanPointer = bucket.getLeft(currentItem.getIndexInsidePage());
      }

      if (parentItem.isLeftChild()) {
        final int rightSiblingPageIndex = parentBucket.getRight(parentItem.getIndexInsidePage());
        try (final OCacheEntry rightSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true)) {
          final CellBTreeSingleValueBucketV3<K> rightSiblingBucket =
              new CellBTreeSingleValueBucketV3<>(rightSiblingEntry);

          final int rightSiblingBucketSize = rightSiblingBucket.size();
          if (rightSiblingBucketSize > 1) {
            return rotateNoneLeafLeftAndRemoveItem(
                parentItem, parentBucket, bucket, rightSiblingBucket, orphanPointer);
          } else if (rightSiblingBucketSize == 1) {
            return mergeNoneLeafWithRightSiblingAndRemoveItem(
                atomicOperation,
                parentItem,
                parentBucket,
                bucket,
                rightSiblingBucket,
                orphanPointer,
                path);
          }

          return false;
        }
      } else {
        final int leftSiblingPageIndex = parentBucket.getLeft(parentItem.getIndexInsidePage());
        try (final OCacheEntry leftSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, leftSiblingPageIndex, true)) {
          final CellBTreeSingleValueBucketV3<K> leftSiblingBucket =
              new CellBTreeSingleValueBucketV3<>(leftSiblingEntry);
          assert leftSiblingBucket.size() > 0;

          final int leftSiblingBucketSize = leftSiblingBucket.size();
          if (leftSiblingBucketSize > 1) {
            return rotateNoneLeafRightAndRemoveItem(
                parentItem, parentBucket, bucket, leftSiblingBucket, orphanPointer);
          } else if (leftSiblingBucketSize == 1) {
            return mergeNoneLeafWithLeftSiblingAndRemoveItem(
                atomicOperation,
                parentItem,
                parentBucket,
                bucket,
                leftSiblingBucket,
                orphanPointer,
                path);
          }

          return false;
        }
      }
    }
  }

  private boolean rotateNoneLeafRightAndRemoveItem(
      final RemovalPathItem parentItem,
      final CellBTreeSingleValueBucketV3<K> parentBucket,
      final CellBTreeSingleValueBucketV3<K> bucket,
      final CellBTreeSingleValueBucketV3<K> leftSibling,
      final int orhanPointer) {
    if (bucket.size() != 1 || leftSibling.size() <= 1) {
      throw new CellBTreeSingleValueV3Exception("BTree is broken", this);
    }

    final byte[] bucketKey = parentBucket.getRawKey(parentItem.getIndexInsidePage(), keySerializer);
    final int leftSiblingSize = leftSibling.size();

    final byte[] separatorKey = leftSibling.getRawKey(leftSiblingSize - 1, keySerializer);

    if (!parentBucket.updateKey(parentItem.getIndexInsidePage(), separatorKey, keySerializer)) {
      return false;
    }

    final int bucketLeft = leftSibling.getRight(leftSiblingSize - 1);

    leftSibling.removeNonLeafEntry(leftSiblingSize - 1, false, keySerializer);

    bucket.removeNonLeafEntry(0, true, keySerializer);

    final boolean result = bucket.addNonLeafEntry(0, bucketLeft, orhanPointer, bucketKey);
    assert result;

    return true;
  }

  private boolean rotateNoneLeafLeftAndRemoveItem(
      final RemovalPathItem parentItem,
      final CellBTreeSingleValueBucketV3<K> parentBucket,
      final CellBTreeSingleValueBucketV3<K> bucket,
      final CellBTreeSingleValueBucketV3<K> rightSibling,
      final int orphanPointer) {
    if (bucket.size() != 1 || rightSibling.size() <= 1) {
      throw new CellBTreeSingleValueV3Exception("BTree is broken", this);
    }

    final byte[] bucketKey = parentBucket.getRawKey(parentItem.getIndexInsidePage(), keySerializer);

    final byte[] separatorKey = rightSibling.getRawKey(0, keySerializer);

    if (!parentBucket.updateKey(parentItem.getIndexInsidePage(), separatorKey, keySerializer)) {
      return false;
    }

    final int bucketRight = rightSibling.getLeft(0);
    bucket.removeNonLeafEntry(0, true, keySerializer);

    final boolean result = bucket.addNonLeafEntry(0, orphanPointer, bucketRight, bucketKey);
    assert result;

    rightSibling.removeNonLeafEntry(0, true, keySerializer);

    return true;
  }

  private boolean mergeNoneLeafWithRightSiblingAndRemoveItem(
      final OAtomicOperation atomicOperation,
      final RemovalPathItem parentItem,
      final CellBTreeSingleValueBucketV3<K> parentBucket,
      final CellBTreeSingleValueBucketV3<K> bucket,
      final CellBTreeSingleValueBucketV3<K> rightSibling,
      final int orphanPointer,
      final List<RemovalPathItem> path)
      throws IOException {

    if (rightSibling.size() != 1 || bucket.size() != 1) {
      throw new CellBTreeSingleValueV3Exception("BTree is broken", this);
    }

    final byte[] key = parentBucket.getRawKey(parentItem.getIndexInsidePage(), keySerializer);
    final boolean success =
        deleteFromNonLeafNode(atomicOperation, parentBucket, path.subList(0, path.size() - 1));

    if (success) {
      final int rightChild = rightSibling.getLeft(0);

      final boolean result = rightSibling.addNonLeafEntry(0, orphanPointer, rightChild, key);
      assert result;

      addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

      return true;
    }

    return false;
  }

  private boolean mergeNoneLeafWithLeftSiblingAndRemoveItem(
      final OAtomicOperation atomicOperation,
      final RemovalPathItem parentItem,
      final CellBTreeSingleValueBucketV3<K> parentBucket,
      final CellBTreeSingleValueBucketV3<K> bucket,
      final CellBTreeSingleValueBucketV3<K> leftSibling,
      final int orphanPointer,
      final List<RemovalPathItem> path)
      throws IOException {

    if (leftSibling.size() != 1 || bucket.size() != 1) {
      throw new CellBTreeSingleValueV3Exception("BTree is broken", this);
    }

    final byte[] key = parentBucket.getRawKey(parentItem.getIndexInsidePage(), keySerializer);

    final boolean success =
        deleteFromNonLeafNode(atomicOperation, parentBucket, path.subList(0, path.size() - 1));

    if (success) {
      final int leftChild = leftSibling.getRight(0);
      final boolean result = leftSibling.addNonLeafEntry(1, leftChild, orphanPointer, key);
      assert result;

      addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

      return true;
    }

    return false;
  }

  private void removePagesStoredInFreeList(
      OAtomicOperation atomicOperation, Set<Integer> pages, int filledUpTo) throws IOException {
    final int freeListHead;

    try (final OCacheEntry entryCacheEntry =
        loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX)) {
      final CellBTreeSingleValueEntryPointV3<K> entryPoint =
          new CellBTreeSingleValueEntryPointV3<>(entryCacheEntry);
      assert entryPoint.getPagesSize() == filledUpTo - 1;
      freeListHead = entryPoint.getFreeListHead();
    }

    int freePageIndex = freeListHead;
    while (freePageIndex >= 0) {
      pages.remove(freePageIndex);

      try (final OCacheEntry cacheEntry =
          loadPageForRead(atomicOperation, fileId, freePageIndex); ) {
        final CellBTreeSingleValueBucketV3<K> bucket =
            new CellBTreeSingleValueBucketV3<>(cacheEntry);
        freePageIndex = bucket.getNextFreeListPage();
        pages.remove(freePageIndex);
      }
    }
  }

  void assertFreePages(OAtomicOperation atomicOperation) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final Set<Integer> pages = new HashSet<>();
        final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

        for (int i = 2; i < filledUpTo; i++) {
          pages.add(i);
        }

        removeUsedPages((int) ROOT_INDEX, pages, atomicOperation);

        removePagesStoredInFreeList(atomicOperation, pages, filledUpTo);

        assert pages.isEmpty();

      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during checking  of btree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private void removeUsedPages(
      final int pageIndex, final Set<Integer> pages, final OAtomicOperation atomicOperation)
      throws IOException {

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
      if (bucket.isLeaf()) {
        return;
      }

      final int bucketSize = bucket.size();
      final List<Integer> pagesToExplore = new ArrayList<>(bucketSize);

      if (bucketSize > 0) {
        final int leftPage = bucket.getLeft(0);
        pages.remove(leftPage);
        pagesToExplore.add(leftPage);

        for (int i = 0; i < bucketSize; i++) {
          final int rightPage = bucket.getRight(i);
          pages.remove(rightPage);
          pagesToExplore.add(rightPage);
        }
      }

      for (final int pageToExplore : pagesToExplore) {
        removeUsedPages(pageToExplore, pages, atomicOperation);
      }
    }
  }

  private void addToFreeList(OAtomicOperation atomicOperation, int pageIndex) throws IOException {
    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
      final CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);

      try (final OCacheEntry entryPointEntry =
          loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {

        final CellBTreeSingleValueEntryPointV3<K> entryPoint =
            new CellBTreeSingleValueEntryPointV3<>(entryPointEntry);

        final int freeListHead = entryPoint.getFreeListHead();
        entryPoint.setFreeListHead(pageIndex);
        bucket.setNextFreeListPage(freeListHead);
      }
    }
  }

  private ORID removeNullBucket(final OAtomicOperation atomicOperation) throws IOException {
    ORID removedValue;
    try (final OCacheEntry nullCacheEntry =
        loadPageForWrite(atomicOperation, nullBucketFileId, 0, true)) {
      final CellBTreeSingleValueV3NullBucket nullBucket =
          new CellBTreeSingleValueV3NullBucket(nullCacheEntry);
      removedValue = nullBucket.getValue();

      if (removedValue != null) {
        nullBucket.removeValue();
      }
    }

    if (removedValue != null) {
      updateSize(-1, atomicOperation);
    }
    return removedValue;
  }

  public Stream<ORawPair<K, ORID>> iterateEntriesMinor(
      final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (!ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMinorDesc(key, inclusive), false);
        }

        return StreamSupport.stream(iterateEntriesMinorAsc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<K, ORID>> iterateEntriesMajor(
      final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMajorAsc(key, inclusive), false);
        }
        return StreamSupport.stream(iterateEntriesMajorDesc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = firstItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();

        try (final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.getPageIndex())) {
          final CellBTreeSingleValueBucketV3<K> bucket =
              new CellBTreeSingleValueBucketV3<>(cacheEntry);
          return bucket.getKey(result.getItemIndex(), keySerializer);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during finding first key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = lastItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();
        try (final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.getPageIndex())) {
          final CellBTreeSingleValueBucketV3<K> bucket =
              new CellBTreeSingleValueBucketV3<>(cacheEntry);
          return bucket.getKey(result.getItemIndex(), keySerializer);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during finding last key in sbtree [" + getName() + "]", this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<K> keyStream() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        //noinspection resource
        return StreamSupport.stream(
                new SpliteratorForward<K>(this, null, null, false, false), false)
            .map((entry) -> entry.first);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<K, ORID>> allEntries() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        //noinspection resource
        return StreamSupport.stream(
            new SpliteratorForward<K>(this, null, null, false, false), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<K, ORID>> iterateEntriesBetween(
      final K keyFrom,
      final boolean fromInclusive,
      final K keyTo,
      final boolean toInclusive,
      final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(
              iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        } else {
          return StreamSupport.stream(
              iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * SB-tree.
   */
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final CellBTreeSingleValueEntryPointV3<K> entryPoint =
          new CellBTreeSingleValueEntryPointV3<>(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    }
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesMinorDesc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new SpliteratorBackward<K>(this, null, key, false, inclusive);
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesMinorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    return new SpliteratorForward<K>(this, null, key, false, inclusive);
  }

  private K enhanceCompositeKeyMinorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMinorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesMajorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new SpliteratorForward<K>(this, key, null, inclusive, false);
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesMajorDesc(K key, final boolean inclusive) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMajorDesc(key, inclusive);

      return new SpliteratorBackward<K>(this, key, null, inclusive, false);

    } finally {
      releaseSharedLock();
    }
  }

  private K enhanceCompositeKeyMajorAsc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMajorDesc(K key, final boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive) {
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private Optional<BucketSearchResult> firstItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);
    int itemIndex = 0;
    try {
      CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() + 1;
            } else {
              return Optional.empty();
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex < bucket.size()) {
              bucketIndex = bucket.getLeft(itemIndex);
            } else {
              bucketIndex = bucket.getRight(itemIndex - 1);
            }

            itemIndex = 0;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() + 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(0, bucketIndex));
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);
        //noinspection ObjectAllocationInLoop
        bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
      }
    } finally {
      cacheEntry.close();
    }
  }

  private Optional<BucketSearchResult> lastItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

    CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() - 1;
            } else {
              return Optional.empty();
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              bucketIndex = bucket.getRight(itemIndex);
            } else {
              bucketIndex = bucket.getLeft(0);
            }

            itemIndex = CellBTreeSingleValueBucketV3.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() - 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(bucket.size() - 1, bucketIndex));
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

        //noinspection ObjectAllocationInLoop
        bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
        if (itemIndex == CellBTreeSingleValueBucketV3.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      cacheEntry.close();
    }
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesBetweenAscOrder(
      K keyFrom, final boolean fromInclusive, K keyTo, final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new SpliteratorForward<K>(this, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private Spliterator<ORawPair<K, ORID>> iterateEntriesBetweenDescOrder(
      K keyFrom, final boolean fromInclusive, K keyTo, final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new SpliteratorBackward<K>(this, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private K enhanceToCompositeKeyBetweenAsc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenAsc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private K enhanceToCompositeKeyBetweenDesc(K keyTo, final boolean toInclusive) {
    final PartialSearchMode partialSearchModeTo;
    if (toInclusive) {
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    } else {
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;
    }

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenDesc(K keyFrom, final boolean fromInclusive) {
    final PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) {
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    } else {
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;
    }

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private UpdateBucketSearchResult splitBucket(
      final CellBTreeSingleValueBucketV3<K> bucketToSplit,
      final OCacheEntry entryToSplit,
      final List<Long> path,
      final List<Integer> itemPointers,
      final int keyIndex,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit = bucketSize >>> 1;
    final K separationKey = bucketToSplit.getKey(indexToSplit, keySerializer);
    final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    for (int i = startRightIndex; i < bucketSize; i++) {
      rightEntries.add(bucketToSplit.getRawEntry(i, keySerializer));
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(
          path,
          itemPointers,
          keyIndex,
          entryToSplit.getPageIndex(),
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    } else {
      return splitRootBucket(
          keyIndex,
          entryToSplit,
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    }
  }

  private UpdateBucketSearchResult splitNonRootBucket(
      final List<Long> path,
      final List<Integer> itemPointers,
      final int keyIndex,
      final long pageIndex,
      final CellBTreeSingleValueBucketV3<K> bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final K separationKey,
      final List<byte[]> rightEntries,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final OCacheEntry rightBucketEntry = allocateNewPage(atomicOperation);
    try {
      final CellBTreeSingleValueBucketV3<K> newRightBucket =
          new CellBTreeSingleValueBucketV3<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries, keySerializer);

      bucketToSplit.shrink(indexToSplit, keySerializer);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          try (final OCacheEntry rightSiblingBucketEntry =
              loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true)) {
            final CellBTreeSingleValueBucketV3<K> rightSiblingBucket =
                new CellBTreeSingleValueBucketV3<>(rightSiblingBucketEntry);
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          }
        }
      }
      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
      try {
        CellBTreeSingleValueBucketV3<K> parentBucket =
            new CellBTreeSingleValueBucketV3<>(parentCacheEntry);
        int insertionIndex = itemPointers.get(itemPointers.size() - 2);
        List<Long> currentPath = path.subList(0, path.size() - 1);
        List<Integer> currentIndex = itemPointers.subList(0, itemPointers.size() - 1);
        while (!parentBucket.addNonLeafEntry(
            insertionIndex,
            (int) pageIndex,
            rightBucketEntry.getPageIndex(),
            keySerializer.serializeNativeAsWhole(separationKey, (Object[]) keyTypes))) {
          final UpdateBucketSearchResult bucketSearchResult =
              splitBucket(
                  parentBucket,
                  parentCacheEntry,
                  currentPath,
                  currentIndex,
                  insertionIndex,
                  atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          insertionIndex = bucketSearchResult.getItemIndex();
          currentPath = bucketSearchResult.getPath();
          currentIndex = bucketSearchResult.getInsertionIndexes();

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            parentCacheEntry.close();

            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
          }

          //noinspection ObjectAllocationInLoop
          parentBucket = new CellBTreeSingleValueBucketV3<>(parentCacheEntry);
        }

      } finally {
        parentCacheEntry.close();
      }

    } finally {
      rightBucketEntry.close();
    }

    final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
    final ArrayList<Integer> resultItemPointers =
        new ArrayList<>(itemPointers.subList(0, itemPointers.size() - 1));

    if (keyIndex <= indexToSplit) {
      resultPath.add(pageIndex);
      resultItemPointers.add(keyIndex);

      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
    }

    final int parentIndex = resultItemPointers.size() - 1;
    resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
    resultPath.add((long) rightBucketEntry.getPageIndex());

    if (splitLeaf) {
      resultItemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
    }

    resultItemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(
        resultItemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private OCacheEntry allocateNewPage(OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry rightBucketEntry;
    try (final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final CellBTreeSingleValueEntryPointV3<K> entryPoint =
          new CellBTreeSingleValueEntryPointV3<>(entryPointCacheEntry);
      final int freeListHead = entryPoint.getFreeListHead();
      if (freeListHead > -1) {
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, freeListHead, false);
        final CellBTreeSingleValueBucketV3<?> bucket =
            new CellBTreeSingleValueBucketV3<>(rightBucketEntry);
        entryPoint.setFreeListHead(bucket.getNextFreeListPage());
      } else {
        int pageSize = entryPoint.getPagesSize();
        if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
          pageSize++;
          rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false);
          entryPoint.setPagesSize(pageSize);
        } else {
          assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

          rightBucketEntry = addPage(atomicOperation, fileId);
          entryPoint.setPagesSize(rightBucketEntry.getPageIndex());
        }
      }
    }

    return rightBucketEntry;
  }

  private UpdateBucketSearchResult splitRootBucket(
      final int keyIndex,
      final OCacheEntry bucketEntry,
      CellBTreeSingleValueBucketV3<K> bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final K separationKey,
      final List<byte[]> rightEntries,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getRawEntry(i, keySerializer));
    }

    final OCacheEntry leftBucketEntry;
    final OCacheEntry rightBucketEntry;

    leftBucketEntry = allocateNewPage(atomicOperation);
    rightBucketEntry = allocateNewPage(atomicOperation);

    try {
      final CellBTreeSingleValueBucketV3<K> newLeftBucket =
          new CellBTreeSingleValueBucketV3<>(leftBucketEntry);
      newLeftBucket.init(splitLeaf);
      newLeftBucket.addAll(leftEntries, keySerializer);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      leftBucketEntry.close();
    }

    try {
      final CellBTreeSingleValueBucketV3<K> newRightBucket =
          new CellBTreeSingleValueBucketV3<>(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries, keySerializer);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      rightBucketEntry.close();
    }

    bucketToSplit = new CellBTreeSingleValueBucketV3<>(bucketEntry);
    bucketToSplit.shrink(0, keySerializer);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }
    bucketToSplit.addNonLeafEntry(
        0,
        leftBucketEntry.getPageIndex(),
        rightBucketEntry.getPageIndex(),
        keySerializer.serializeNativeAsWhole(separationKey, (Object[]) keyTypes));

    final ArrayList<Long> resultPath = new ArrayList<>(8);
    resultPath.add(ROOT_INDEX);

    final ArrayList<Integer> itemPointers = new ArrayList<>(8);

    if (keyIndex <= indexToSplit) {
      itemPointers.add(-1);
      itemPointers.add(keyIndex);

      resultPath.add((long) leftBucketEntry.getPageIndex());
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
    }

    resultPath.add((long) rightBucketEntry.getPageIndex());
    itemPointers.add(0);

    if (splitLeaf) {
      itemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
    }

    itemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private Optional<RemoveSearchResult> findBucketForRemove(
      final K key, final OAtomicOperation atomicOperation) throws IOException {

    final ArrayList<RemovalPathItem> path = new ArrayList<>(8);

    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new CellBTreeSingleValueV3Exception(
            "We reached max level of depth of BTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      try (final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final CellBTreeSingleValueBucketV3<K> bucket =
            new CellBTreeSingleValueBucketV3<>(bucketEntry);

        final int index = bucket.find(key, keySerializer);

        if (bucket.isLeaf()) {
          if (index < 0) {
            return Optional.empty();
          }

          return Optional.of(new RemoveSearchResult(pageIndex, index, path));
        }

        if (index >= 0) {
          path.add(new RemovalPathItem(pageIndex, index, false));

          pageIndex = bucket.getRight(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= bucket.size()) {
            path.add(new RemovalPathItem(pageIndex, insertionIndex - 1, false));

            pageIndex = bucket.getRight(insertionIndex - 1);
          } else {
            path.add(new RemovalPathItem(pageIndex, insertionIndex, true));

            pageIndex = bucket.getLeft(insertionIndex);
          }
        }
      }
    }
  }

  private BucketSearchResult findBucket(final K key, final OAtomicOperation atomicOperation)
      throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new CellBTreeSingleValueV3Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      try (final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final CellBTreeSingleValueBucketV3<K> keyBucket =
            new CellBTreeSingleValueBucketV3<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer);

        if (keyBucket.isLeaf()) {
          return new BucketSearchResult(index, pageIndex);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }
        }
      }
    }
  }

  private UpdateBucketSearchResult findBucketForUpdate(
      final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    final ArrayList<Long> path = new ArrayList<>(8);
    final ArrayList<Integer> itemIndexes = new ArrayList<>(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new CellBTreeSingleValueV3Exception(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);
      try (final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final CellBTreeSingleValueBucketV3<K> keyBucket =
            new CellBTreeSingleValueBucketV3<>(bucketEntry);
        final int index = keyBucket.find(key, keySerializer);

        if (keyBucket.isLeaf()) {
          itemIndexes.add(index);
          return new UpdateBucketSearchResult(itemIndexes, path, index);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
          itemIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }

          itemIndexes.add(insertionIndex);
        }
      }
    }
  }

  private K enhanceCompositeKey(final K key, final PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey)) {
      return key;
    }

    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (!(keySize == 1
        || compositeKey.getKeys().size() == keySize
        || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      final int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY)) {
        keyItem = ALWAYS_GREATER_KEY;
      } else {
        keyItem = ALWAYS_LESS_KEY;
      }

      for (int i = 0; i < itemsToAdd; i++) {
        fullKey.addKey(keyItem);
      }

      //noinspection unchecked
      return (K) fullKey;
    }

    return key;
  }

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of
   * internal keys are used, whether lowest or highest partially matched key should be used.
   */
  private enum PartialSearchMode {
    /** Any partially matched key will be used as search result. */
    NONE,
    /** The biggest partially matched key will be used as search result. */
    HIGHEST_BOUNDARY,

    /** The smallest partially matched key will be used as search result. */
    LOWEST_BOUNDARY
  }

  public void fetchBackwardNextCachePortion(SpliteratorBackward<K> iter) {
    final K lastKey;
    if (iter.getDataCache().isEmpty()) {
      lastKey = null;
    } else {
      lastKey = iter.getDataCache().get(iter.getDataCache().size() - 1).first;
    }

    iter.getDataCache().clear();
    iter.setCacheIterator(Collections.emptyIterator());

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (iter.getPageIndex() > -1) {
          if (readKeysFromBucketsBackward(atomicOperation, iter)) {
            return;
          }
        }

        // this can only happen if page LSN does not equal to stored LSN or index of current
        // iterated page equals to -1
        // so we only started iteration
        if (iter.getDataCache().isEmpty()) {
          // iteration just started
          if (lastKey == null) {
            if (iter.getToKey() != null) {
              final BucketSearchResult searchResult = findBucket(iter.getToKey(), atomicOperation);
              iter.setPageIndex((int) searchResult.getPageIndex());

              if (searchResult.getItemIndex() >= 0) {
                if (iter.isToKeyInclusive()) {
                  iter.setItemIndex(searchResult.getItemIndex());
                } else {
                  iter.setItemIndex(searchResult.getItemIndex() - 1);
                }
              } else {
                iter.setItemIndex(-searchResult.getItemIndex() - 2);
              }
            } else {
              final Optional<BucketSearchResult> bucketSearchResult = lastItem(atomicOperation);
              if (bucketSearchResult.isPresent()) {
                final BucketSearchResult searchResult = bucketSearchResult.get();
                iter.setPageIndex((int) searchResult.getPageIndex());
                iter.setItemIndex(searchResult.getItemIndex());
              } else {
                return;
              }
            }

          } else {
            final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

            iter.setPageIndex((int) bucketSearchResult.getPageIndex());
            if (bucketSearchResult.getItemIndex() >= 0) {
              iter.setItemIndex(bucketSearchResult.getItemIndex() - 1);
            } else {
              iter.setItemIndex(-bucketSearchResult.getItemIndex() - 2);
            }
          }
          iter.setLastLSN(null);
          readKeysFromBucketsBackward(atomicOperation, iter);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception("Error during element iteration", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  void fetchNextForwardCachePortion(SpliteratorForward<K> iter) {
    final K lastKey;
    if (!iter.getDataCache().isEmpty()) {
      lastKey = iter.getDataCache().get(iter.getDataCache().size() - 1).first;
    } else {
      lastKey = null;
    }

    iter.getDataCache().clear();
    iter.setCacheIterator(Collections.emptyIterator());

    atomicOperationsManager.acquireReadLock(CellBTreeSingleValueV3.this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (iter.getPageIndex() > -1) {
          if (readKeysFromBucketsForward(atomicOperation, iter)) {
            return;
          }
        }

        // this can only happen if page LSN does not equal to stored LSN or index of current
        // iterated page equals to -1
        // so we only started iteration
        if (iter.getDataCache().isEmpty()) {
          // iteration just started
          if (lastKey == null) {
            if (iter.getFromKey() != null) {
              final BucketSearchResult searchResult =
                  findBucket(iter.getFromKey(), atomicOperation);
              iter.setPageIndex((int) searchResult.getPageIndex());

              if (searchResult.getItemIndex() >= 0) {
                if (iter.isFromKeyInclusive()) {
                  iter.setItemIndex(searchResult.getItemIndex());
                } else {
                  iter.setItemIndex(searchResult.getItemIndex() + 1);
                }
              } else {
                iter.setItemIndex(-searchResult.getItemIndex() - 1);
              }
            } else {
              final Optional<BucketSearchResult> bucketSearchResult = firstItem(atomicOperation);
              if (bucketSearchResult.isPresent()) {
                final BucketSearchResult searchResult = bucketSearchResult.get();
                iter.setPageIndex((int) searchResult.getPageIndex());
                iter.setItemIndex(searchResult.getItemIndex());
              } else {
                return;
              }
            }

          } else {
            final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

            iter.setPageIndex((int) bucketSearchResult.getPageIndex());
            if (bucketSearchResult.getItemIndex() >= 0) {
              iter.setItemIndex(bucketSearchResult.getItemIndex() + 1);
            } else {
              iter.setItemIndex(-bucketSearchResult.getItemIndex() - 1);
            }
          }
          iter.setLastLSN(null);
          readKeysFromBucketsForward(atomicOperation, iter);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new CellBTreeSingleValueV3Exception(
              "Error during element iteration", CellBTreeSingleValueV3.this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(CellBTreeSingleValueV3.this);
    }
  }

  private boolean readKeysFromBucketsForward(
      OAtomicOperation atomicOperation, SpliteratorForward<K> iter) throws IOException {
    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
    try {
      CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
      if (iter.getLastLSN() == null || bucket.getLSN().equals(iter.getLastLSN())) {
        while (true) {
          int bucketSize = bucket.size();
          if (iter.getItemIndex() >= bucketSize) {
            iter.setPageIndex((int) bucket.getRightSibling());

            if (iter.getPageIndex() < 0) {
              return true;
            }

            iter.setItemIndex(0);
            cacheEntry.close();

            cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
            bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);

            bucketSize = bucket.size();
          }

          iter.setLastLSN(bucket.getLSN());

          for (;
              iter.getItemIndex() < bucketSize
                  && iter.getDataCache().size() < SPLITERATOR_CACHE_SIZE;
              iter.setItemIndex(iter.getItemIndex() + 1)) {
            @SuppressWarnings("ObjectAllocationInLoop")
            CellBTreeSingleValueEntryV3<K> entry =
                bucket.getEntry(iter.getItemIndex(), keySerializer);

            if (iter.getToKey() != null) {
              if (iter.isToKeyInclusive()) {
                if (comparator.compare(entry.key, iter.getToKey()) > 0) {
                  return true;
                }
              } else if (comparator.compare(entry.key, iter.getToKey()) >= 0) {
                return true;
              }
            }

            //noinspection ObjectAllocationInLoop
            iter.getDataCache().add(new ORawPair<>(entry.key, entry.value));
          }

          if (iter.getDataCache().size() >= SPLITERATOR_CACHE_SIZE) {
            return true;
          }
        }
      }
    } finally {
      cacheEntry.close();
    }

    return false;
  }

  private boolean readKeysFromBucketsBackward(
      OAtomicOperation atomicOperation, SpliteratorBackward<K> iter) throws IOException {
    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
    try {
      CellBTreeSingleValueBucketV3<K> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
      if (iter.getLastLSN() == null || bucket.getLSN().equals(iter.getLastLSN())) {
        while (true) {
          if (iter.getItemIndex() < 0) {
            iter.setPageIndex((int) bucket.getLeftSibling());

            if (iter.getPageIndex() < 0) {
              return true;
            }

            cacheEntry.close();

            cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
            bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
            final int bucketSize = bucket.size();
            iter.setItemIndex(bucketSize - 1);
          }

          iter.setLastLSN(bucket.getLSN());

          for (;
              iter.getItemIndex() >= 0 && iter.getDataCache().size() < SPLITERATOR_CACHE_SIZE;
              iter.setItemIndex(iter.getItemIndex() - 1)) {
            @SuppressWarnings("ObjectAllocationInLoop")
            CellBTreeSingleValueEntryV3<K> entry =
                bucket.getEntry(iter.getItemIndex(), keySerializer);

            if (iter.getFromKey() != null) {
              if (iter.isFromKeyInclusive()) {
                if (comparator.compare(entry.key, iter.getFromKey()) < 0) {
                  return true;
                }
              } else if (comparator.compare(entry.key, iter.getFromKey()) <= 0) {
                return true;
              }
            }

            //noinspection ObjectAllocationInLoop
            iter.getDataCache().add(new ORawPair<>(entry.key, entry.value));
          }

          if (iter.getDataCache().size() >= SPLITERATOR_CACHE_SIZE) {
            return true;
          }
        }
      }
    } finally {
      cacheEntry.close();
    }

    return false;
  }
}
