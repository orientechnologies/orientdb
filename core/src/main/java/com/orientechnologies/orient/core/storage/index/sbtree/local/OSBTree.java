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

package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.util.*;

/**
 * This is implementation which is based on B+-tree implementation threaded tree.
 * The main differences are:
 * <ol>
 * <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused later when new items are added.</li>
 * <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more than keys contained in current
 * bucket</li>
 * <ol/>
 * There is support of null values for keys, but values itself cannot be null. Null keys support is switched off by default if null
 * keys are supported value which is related to null key will be stored in separate file which has only one page.
 * Buckets/pages for usual (non-null) key-value entries can be considered as sorted array. The first bytes of page contains such
 * auxiliary information as size of entries contained in bucket, links to neighbors which contain entries with keys less/more than
 * keys in current bucket.
 * The next bytes contain sorted array of entries. Array itself is split on two parts. First part is growing from start to end, and
 * second part is growing from end to start.
 * First part is array of offsets to real key-value entries which are stored in second part of array which grows from end to start.
 * This array of offsets is sorted by accessing order according to key value. So we can use binary search to find requested key.
 * When new key-value pair is added we append binary presentation of this pair to the second part of array which grows from end of
 * page to start, remember value of offset for this pair, and find proper position of this offset inside of first part of array.
 * Such approach allows to minimize amount of memory involved in performing of operations and as result speed up data processing.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public class OSBTree<K, V> extends ODurableComponent {
  private static final int               MAX_KEY_SIZE            = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final int               MAX_EMBEDDED_VALUE_SIZE = OGlobalConfiguration.SBTREE_MAX_EMBEDDED_VALUE_SIZE
      .getValueAsInteger();
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY         = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY      = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH = OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private final static long                  ROOT_INDEX = 0;
  private final        Comparator<? super K> comparator = ODefaultComparator.INSTANCE;
  private final String  nullFileExtension;
  private       long    fileId;
  private long nullBucketFileId = -1;
  private int                  keySize;
  private OBinarySerializer<K> keySerializer;
  private OType[]              keyTypes;
  private OBinarySerializer<V> valueSerializer;
  private boolean              nullPointerSupport;

  public OSBTree(String name, String dataFileExtension, String nullFileExtension, OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes, int keySize,
      boolean nullPointerSupport) {
    assert keySerializer != null;
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(false);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree creation", this), e);
      }

      acquireExclusiveLock();
      try {

        this.keySize = keySize;
        if (keyTypes != null)
          this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
        else
          this.keyTypes = null;

        this.keySerializer = keySerializer;

        this.valueSerializer = valueSerializer;
        this.nullPointerSupport = nullPointerSupport;

        fileId = addFile(atomicOperation, getFullName());

        if (nullPointerSupport)
          nullBucketFileId = addFile(atomicOperation, getName() + nullFileExtension);

        OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId);
        try {

          OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, true, keySerializer, keyTypes, valueSerializer);
          rootBucket.setTreeSize(0);

        } finally {
          releasePageFromWrite(atomicOperation, rootCacheEntry);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        try {
          endAtomicOperation(true, e);
        } catch (IOException e1) {
          OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
        }
        throw OException.wrapException(new OSBTreeException("Error creation of sbtree with name " + getName(), this), e);
      } catch (RuntimeException e) {
        try {
          endAtomicOperation(true, e);
        } catch (IOException e1) {
          OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
        }
        throw e;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public boolean isNullPointerSupport() {
    acquireSharedLock();
    try {
      return nullPointerSupport;
    } finally {
      releaseSharedLock();
    }
  }

  public V get(K key) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          checkNullSupport(key);

          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          if (key != null) {
            key = keySerializer.preprocess(key, (Object[]) keyTypes);

            BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
            if (bucketSearchResult.itemIndex < 0)
              return null;

            long pageIndex = bucketSearchResult.getLastPathItem();
            OCacheEntry keyBucketCacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes,
                  valueSerializer);

              OSBTreeBucket.SBTreeEntry<K, V> treeEntry = keyBucket.getEntry(bucketSearchResult.itemIndex);
              return readValue(treeEntry.value, atomicOperation);
            } finally {
              releasePageFromRead(atomicOperation, keyBucketCacheEntry);
            }
          } else {
            if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0)
              return null;

            final OCacheEntry nullBucketCacheEntry = loadPageForRead(atomicOperation, nullBucketFileId, 0, false);
            try {
              final ONullBucket<V> nullBucket = new ONullBucket<V>(nullBucketCacheEntry, valueSerializer, false);
              final OSBTreeValue<V> treeValue = nullBucket.getValue();
              if (treeValue == null)
                return null;

              return readValue(treeValue, atomicOperation);
            } finally {
              releasePageFromRead(atomicOperation, nullBucketCacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during retrieving  of sbtree with name " + getName(), this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.startIndexEntryReadTimer();
      completeOperation();
    }
  }

  public void put(K key, V value) {
    put(key, value, null);
  }

  public boolean validatedPut(K key, V value, OIndexEngine.Validator<K, V> validator) {
    return put(key, value, validator);
  }

  private boolean put(K key, V value, OIndexEngine.Validator<K, V> validator) {
    return update(key, (x) -> {
      return OIndexUpdateAction.changed(value);
    }, validator);
  }

  @SuppressWarnings("unchecked")
  public boolean update(K key, OIndexKeyUpdater<V> updater, OIndexEngine.Validator<K, V> validator) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryUpdateTimer();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree entrie put", this), e);
      }

      acquireExclusiveLock();
      try {
        checkNullSupport(key);

        if (key != null) {
          final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);

          if (keySize > MAX_KEY_SIZE)
            throw new OTooBigIndexKeyException(
                "Key size is more than allowed, operation was canceled. Current key size " + keySize + ", allowed  " + MAX_KEY_SIZE,
                getName());

          key = keySerializer.preprocess(key, (Object[]) keyTypes);
          long valueLink = -1;

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

          OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false);
          OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);
          final V oldValue = bucketSearchResult.itemIndex > -1 ?
              readValue(keyBucket.getValue(bucketSearchResult.itemIndex), atomicOperation) :
              null;
          OIndexUpdateAction<V> updatedValue = updater.update(oldValue);
          final OSBTreeValue<V> treeValue;
          if (updatedValue.isChange()) {
            V value = updatedValue.getValue();
            final int valueSize = valueSerializer.getObjectSize(value);
            final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;
            if (createLinkToTheValue)
              valueLink = createLinkToTheValue(value, atomicOperation);

            if (validator != null) {
              boolean failure = true; // assuming validation throws by default
              boolean ignored = false;

              try {

                final Object result = validator.validate(key, oldValue, value);
                if (result == OIndexEngine.Validator.IGNORE) {
                  ignored = true;
                  failure = false;
                  return false;
                }

                value = (V) result;
                failure = false;
              } finally {
                if (failure || ignored) {
                  releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
                }
                if (ignored) // in case of a failure atomic operation will be ended in a usual way below
                  endAtomicOperation(false, null);
              }
            }
            treeValue = new OSBTreeValue<V>(createLinkToTheValue, valueLink, createLinkToTheValue ? null : value);
            int insertionIndex;
            int sizeDiff;
            if (bucketSearchResult.itemIndex >= 0) {

              int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, treeValue);

              if (updateResult >= 0) {
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

                endAtomicOperation(false, null);
                return true;
              } else {
                assert updateResult == -1;

                long removedLinkedValue = keyBucket.remove(bucketSearchResult.itemIndex);
                if (removedLinkedValue >= 0)
                  removeLinkedValue(removedLinkedValue, atomicOperation);

                insertionIndex = bucketSearchResult.itemIndex;
                sizeDiff = 0;
              }
            } else {
              insertionIndex = -bucketSearchResult.itemIndex - 1;
              sizeDiff = 1;
            }

            while (!keyBucket.addEntry(insertionIndex, new OSBTreeBucket.SBTreeEntry<K, V>(-1, -1, key, treeValue), true)) {
              releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

              bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);

              insertionIndex = bucketSearchResult.itemIndex;

              keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false);

              keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);
            }

            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

            if (sizeDiff != 0) {
              updateSize(sizeDiff, atomicOperation);
            }
          } else if (updatedValue.isRemove()) {
            removeKey(atomicOperation, bucketSearchResult);
            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
          }
        } else {
          OCacheEntry cacheEntry;
          boolean isNew = false;

          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            cacheEntry = addPage(atomicOperation, nullBucketFileId);
            isNew = true;
          } else
            cacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false);

          int sizeDiff = 0;

          boolean ignored = false;
          try {
            final ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, valueSerializer, isNew);
            final OSBTreeValue<V> oldValue = nullBucket.getValue();
            final V oldValueValue = oldValue == null ? null : readValue(oldValue, atomicOperation);
            OIndexUpdateAction<V> updatedValue = updater.update(oldValueValue);
            if (updatedValue.isChange()) {
              V value = updatedValue.getValue();
              final int valueSize = valueSerializer.getObjectSize(value);
              final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;

              long valueLink = -1;
              if (createLinkToTheValue)
                valueLink = createLinkToTheValue(value, atomicOperation);

              final OSBTreeValue<V> treeValue = new OSBTreeValue<V>(createLinkToTheValue, valueLink,
                  createLinkToTheValue ? null : value);

              if (validator != null) {

                final Object result = validator.validate(null, oldValueValue, value);
                if (result == OIndexEngine.Validator.IGNORE) {
                  ignored = true;
                  return false;
                }

              }

              if (oldValue != null)
                sizeDiff = -1;

              nullBucket.setValue(treeValue);
            } else {
              removeNullBucket(atomicOperation);
            }
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
            if (ignored)
              endAtomicOperation(false, null);
          }

          sizeDiff++;
          updateSize(sizeDiff, atomicOperation);
        }

        endAtomicOperation(false, null);
        return true;
      } catch (IOException e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeException("Error during index update with key " + key, this), e);
      } catch (RuntimeException e) {
        rollback(e);
        throw e;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryUpdateTimer();
      completeOperation();
    }
  }

  public void close(boolean flush) {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        readCache.closeFile(fileId, flush, writeCache);

        if (nullPointerSupport)
          readCache.closeFile(nullBucketFileId, flush, writeCache);

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void close() {
    close(true);
  }

  public void clear() {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree clear", this), e);
      }

      acquireExclusiveLock();
      try {
        truncateFile(atomicOperation, fileId);

        if (nullPointerSupport)
          truncateFile(atomicOperation, nullBucketFileId);

        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false);
        if (cacheEntry == null) {
          cacheEntry = addPage(atomicOperation, fileId);
        }

        try {
          OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(cacheEntry, true, keySerializer, keyTypes, valueSerializer);

          rootBucket.setTreeSize(0);

        } finally {
          releasePageFromWrite(atomicOperation, cacheEntry);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        rollback(e);

        throw OException.wrapException(new OSBTreeException("Error during clear of sbtree with name " + getName(), this), e);
      } catch (RuntimeException e) {
        rollback(e);
        throw e;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void delete() {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(false);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree deletion", this), e);
      }

      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);

        if (nullPointerSupport)
          deleteFile(atomicOperation, nullBucketFileId);

        endAtomicOperation(false, null);
      } catch (IOException e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeException("Error during delete of sbtree with name " + getName(), this), e);
      } catch (Exception e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeException("Error during delete of sbtree with name " + getName(), this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void deleteWithoutLoad(String name) {
    startOperation();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(false);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree deletion", this), e);
      }

      acquireExclusiveLock();
      try {
        if (isFileExists(atomicOperation, getFullName())) {
          final long fileId = openFile(atomicOperation, getFullName());
          deleteFile(atomicOperation, fileId);
        }

        if (isFileExists(atomicOperation, getName() + nullFileExtension)) {
          final long nullFileId = openFile(atomicOperation, getName() + nullFileExtension);
          deleteFile(atomicOperation, nullFileId);
        }

        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        rollback(ioe);
        throw OException.wrapException(new OSBTreeException("Exception during deletion of sbtree " + getName(), this), ioe);
      } catch (Exception e) {
        rollback(e);
        throw OException.wrapException(new OSBTreeException("Exception during deletion of sbtree " + getName(), this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void load(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      int keySize, boolean nullPointerSupport) {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        this.keySize = keySize;
        if (keyTypes != null)
          this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
        else
          this.keyTypes = null;

        this.nullPointerSupport = nullPointerSupport;

        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        fileId = openFile(atomicOperation, getFullName());
        if (nullPointerSupport)
          nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Exception during loading of sbtree " + name, this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public long size() {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          OCacheEntry rootCacheEntry = loadPageForRead(atomicOperation, fileId, ROOT_INDEX, false);
          try {
            OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
            return rootBucket.getTreeSize();
          } finally {
            releasePageFromRead(atomicOperation, rootCacheEntry);
          }
        } finally {
          releaseSharedLock();
        }
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during retrieving of size of index " + getName(), this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public V remove(K key) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryDeletionTimer();
    try {
      final OAtomicOperation atomicOperation;
      try {
        atomicOperation = startAtomicOperation(true);
      } catch (IOException e) {
        throw OException.wrapException(new OSBTreeException("Error during sbtree entrie remove", this), e);
      }

      acquireExclusiveLock();
      try {
        V removedValue;

        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            endAtomicOperation(false, null);
            return null;
          }

          removedValue = removeKey(atomicOperation, bucketSearchResult);
        } else {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            endAtomicOperation(false, null);
            return null;
          }

          removedValue = removeNullBucket(atomicOperation);
        }

        endAtomicOperation(false, null);
        return removedValue;
      } catch (IOException e) {
        rollback(e);

        throw OException
            .wrapException(new OSBTreeException("Error during removing key " + key + " from sbtree " + getName(), this), e);
      } catch (RuntimeException e) {
        rollback(e);
        throw e;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryDeletionTimer();
      completeOperation();
    }
  }

  public V removeNullBucket(OAtomicOperation atomicOperation) throws IOException {
    V removedValue;
    OCacheEntry nullCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false);
    try {
      ONullBucket<V> nullBucket = new ONullBucket<V>(nullCacheEntry, valueSerializer, false);
      OSBTreeValue<V> treeValue = nullBucket.getValue();

      if (treeValue != null) {
        removedValue = readValue(treeValue, atomicOperation);
        nullBucket.removeValue();
      } else
        removedValue = null;
    } finally {
      releasePageFromWrite(atomicOperation, nullCacheEntry);
    }

    if (removedValue != null)
      updateSize(-1, atomicOperation);
    return removedValue;
  }

  public V removeKey(OAtomicOperation atomicOperation, BucketSearchResult bucketSearchResult) throws IOException {
    V removedValue;
    OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false);
    try {
      OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);

      final OSBTreeValue<V> removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;
      final V value = readValue(removed, atomicOperation);

      long removedValueLink = keyBucket.remove(bucketSearchResult.itemIndex);
      if (removedValueLink >= 0)
        removeLinkedValue(removedValueLink, atomicOperation);

      updateSize(-1, atomicOperation);

      removedValue = value;
    } finally {
      releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
    }
    return removedValue;
  }

  public OSBTreeCursor<K, V> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();

    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          if (!ascSortOrder)
            return iterateEntriesMinorDesc(key, inclusive, atomicOperation);

          return iterateEntriesMinorAsc(key, inclusive, atomicOperation);
        } finally {
          releaseSharedLock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(
            new OSBTreeException("Error during iteration of minor values for key " + key + " in sbtree " + getName(), this), ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          if (ascSortOrder)
            return iterateEntriesMajorAsc(key, inclusive, atomicOperation);

          return iterateEntriesMajorDesc(key, inclusive, atomicOperation);
        } finally {
          releaseSharedLock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(
            new OSBTreeException("Error during iteration of major values for key " + key + " in sbtree " + getName(), this), ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public K firstKey() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final BucketSearchResult searchResult = firstItem(atomicOperation);
          if (searchResult == null)
            return null;

          final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.getLastPathItem(), false);
          try {
            OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);
            return bucket.getKey(searchResult.itemIndex);
          } finally {
            releasePageFromRead(atomicOperation, cacheEntry);
          }
        } finally {
          releaseSharedLock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public K lastKey() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();

    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final BucketSearchResult searchResult = lastItem(atomicOperation);
          if (searchResult == null)
            return null;

          final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.getLastPathItem(), false);
          try {
            OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);
            return bucket.getKey(searchResult.itemIndex);
          } finally {
            releasePageFromRead(atomicOperation, cacheEntry);
          }
        } finally {
          releaseSharedLock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeException("Error during finding last key in sbtree [" + getName() + "]", this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public OSBTreeKeyCursor<K> keyCursor() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();

    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final BucketSearchResult searchResult = firstItem(atomicOperation);
          if (searchResult == null)
            return new OSBTreeKeyCursor<K>() {
              @Override
              public K next(int prefetchSize) {
                return null;
              }
            };

          return new OSBTreeFullKeyCursor(searchResult.getLastPathItem());
        } finally {
          releaseSharedLock();
        }
      } catch (IOException e) {
        throw OException
            .wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      boolean ascSortOrder) {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    startOperation();
    if (statistic != null)
      statistic.startIndexEntryReadTimer();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          if (ascSortOrder)
            return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive, atomicOperation);
          else
            return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive, atomicOperation);
        } finally {
          releaseSharedLock();
        }
      } catch (IOException ioe) {
        throw OException.wrapException(new OSBTreeException(
            "Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + getName(), this), ioe);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      if (statistic != null)
        statistic.stopIndexEntryReadTimer();
      completeOperation();
    }
  }

  public void flush() {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          writeCache.flush();
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this SB-tree.
   */
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  private void checkNullSupport(K key) {
    if (key == null && !nullPointerSupport)
      throw new OSBTreeException("Null keys are not supported.", this);
  }

  private void removeLinkedValue(long removedLink, OAtomicOperation atomicOperation) throws IOException {
    long nextPage = removedLink;
    do {
      removedLink = nextPage;

      OCacheEntry valueEntry = loadPageForRead(atomicOperation, fileId, removedLink, false);
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(valueEntry, false);
        nextPage = valuePage.getNextPage();
      } finally {
        releasePageFromRead(atomicOperation, valueEntry);
      }

      removeValuePage(removedLink, atomicOperation);
    } while (nextPage >= 0);
  }

  private void removeValuePage(long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    long prevFreeListItem;

    OCacheEntry rootCacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false);

    OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
    try {
      prevFreeListItem = rootBucket.getValuesFreeListFirstIndex();
      rootBucket.setValuesFreeListFirstIndex(pageIndex);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }

    OCacheEntry valueEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    try {
      OSBTreeValuePage valuePage = new OSBTreeValuePage(valueEntry, false);
      valuePage.setNextFreeListPage(prevFreeListItem);
    } finally {
      releasePageFromWrite(atomicOperation, valueEntry);
    }
  }

  private long createLinkToTheValue(V value, OAtomicOperation atomicOperation) throws IOException {
    byte[] serializeValue = new byte[valueSerializer.getObjectSize(value)];
    valueSerializer.serializeNativeObject(value, serializeValue, 0);

    final int amountOfPages = OSBTreeValuePage.calculateAmountOfPage(serializeValue.length);

    int position = 0;
    long freeListPageIndex = allocateValuePageFromFreeList(atomicOperation);

    OCacheEntry cacheEntry;
    if (freeListPageIndex < 0)
      cacheEntry = addPage(atomicOperation, fileId);
    else
      cacheEntry = loadPageForWrite(atomicOperation, fileId, freeListPageIndex, false);

    final long valueLink = cacheEntry.getPageIndex();
    try {
      OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, freeListPageIndex >= 0);
      position = valuePage.fillBinaryContent(serializeValue, position);

      valuePage.setNextFreeListPage(-1);
      valuePage.setNextPage(-1);

    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }

    long prevPage = valueLink;
    for (int i = 1; i < amountOfPages; i++) {
      freeListPageIndex = allocateValuePageFromFreeList(atomicOperation);

      if (freeListPageIndex < 0)
        cacheEntry = addPage(atomicOperation, fileId);
      else
        cacheEntry = loadPageForWrite(atomicOperation, fileId, freeListPageIndex, false);

      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, freeListPageIndex >= 0);
        position = valuePage.fillBinaryContent(serializeValue, position);

        valuePage.setNextFreeListPage(-1);
        valuePage.setNextPage(-1);

      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }

      OCacheEntry prevPageCacheEntry = loadPageForWrite(atomicOperation, fileId, prevPage, false);
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(prevPageCacheEntry, freeListPageIndex >= 0);
        valuePage.setNextPage(cacheEntry.getPageIndex());

      } finally {
        releasePageFromWrite(atomicOperation, prevPageCacheEntry);
      }

      prevPage = cacheEntry.getPageIndex();
    }

    return valueLink;
  }

  private long allocateValuePageFromFreeList(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPageForRead(atomicOperation, fileId, ROOT_INDEX, false);
    assert rootCacheEntry != null;

    long freeListFirstIndex;
    OSBTreeBucket<K, V> rootBucket;
    try {
      rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
      freeListFirstIndex = rootBucket.getValuesFreeListFirstIndex();
    } finally {
      releasePageFromRead(atomicOperation, rootCacheEntry);
    }

    if (freeListFirstIndex >= 0) {
      OCacheEntry freePageEntry = loadPageForWrite(atomicOperation, fileId, freeListFirstIndex, false);
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(freePageEntry, false);
        long nextFreeListIndex = valuePage.getNextFreeListPage();

        rootCacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false);
        rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
        try {
          rootBucket.setValuesFreeListFirstIndex(nextFreeListIndex);
        } finally {
          releasePageFromWrite(atomicOperation, rootCacheEntry);
        }

        valuePage.setNextFreeListPage(-1);
      } finally {
        releasePageFromWrite(atomicOperation, freePageEntry);
      }

      return freePageEntry.getPageIndex();
    }

    return -1;
  }

  private void rollback(Exception e) {
    try {
      endAtomicOperation(true, e);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  private void updateSize(long diffSize, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false);
    try {
      OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
      rootBucket.setTreeSize(rootBucket.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }
  }

  private void setSize(long size, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false);
    try {
      OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
      rootBucket.setTreeSize(size);
    } finally {
      releasePageFromWrite(atomicOperation, rootCacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorDesc(K key, boolean inclusive, OAtomicOperation atomicOperation)
      throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

    long pageIndex = bucketSearchResult.getLastPathItem();
    int index;
    if (bucketSearchResult.itemIndex >= 0) {
      index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
    } else {
      index = -bucketSearchResult.itemIndex - 2;
    }

    return new OSBTreeCursorBackward(pageIndex, index, null, key, false, inclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorAsc(K key, boolean inclusive, OAtomicOperation atomicOperation)
      throws IOException {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMinorAsc(key, inclusive);

      final BucketSearchResult searchResult;
      searchResult = firstItem(atomicOperation);
      if (searchResult == null)
        return new OSBTreeCursor<K, V>() {
          @Override
          public Map.Entry<K, V> next(int prefetchSize) {
            return null;
          }
        };

      return new OSBTreeCursorForward(searchResult.getLastPathItem(), searchResult.itemIndex, null, key, false, inclusive);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
    } finally {
      releaseSharedLock();
    }

  }

  private K enhanceCompositeKeyMinorDesc(K key, boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive)
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMinorAsc(K key, boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive)
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private OSBTreeCursor<K, V> iterateEntriesMajorAsc(K key, boolean inclusive, OAtomicOperation atomicOperation)
      throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

    long pageIndex = bucketSearchResult.getLastPathItem();
    int index;
    if (bucketSearchResult.itemIndex >= 0) {
      index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
    } else {
      index = -bucketSearchResult.itemIndex - 1;
    }

    return new OSBTreeCursorForward(pageIndex, index, key, null, inclusive, false);
  }

  private OSBTreeCursor<K, V> iterateEntriesMajorDesc(K key, boolean inclusive, OAtomicOperation atomicOperation)
      throws IOException {
    final BucketSearchResult searchResult;
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMajorDesc(key, inclusive);

      searchResult = lastItem(atomicOperation);
      if (searchResult == null)
        return new OSBTreeCursor<K, V>() {
          @Override
          public Map.Entry<K, V> next(int prefetchSize) {
            return null;
          }
        };

    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding last key in sbtree [" + getName() + "]", this), e);
    } finally {
      releaseSharedLock();
    }

    return new OSBTreeCursorBackward(searchResult.getLastPathItem(), searchResult.itemIndex, key, null, inclusive, false);
  }

  private K enhanceCompositeKeyMajorAsc(K key, boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive)
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private K enhanceCompositeKeyMajorDesc(K key, boolean inclusive) {
    final PartialSearchMode partialSearchMode;
    if (inclusive)
      partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;

    key = enhanceCompositeKey(key, partialSearchMode);
    return key;
  }

  private BucketSearchResult firstItem(OAtomicOperation atomicOperation) throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);
    int itemIndex = 0;
    try {
      OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else
              return null;
          } else {
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex < bucket.size()) {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
              bucketIndex = entry.leftChild;
            } else {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex - 1);
              bucketIndex = entry.rightChild;
            }

            itemIndex = 0;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else
              return null;
          } else {
            final ArrayList<Long> resultPath = new ArrayList<Long>(path.size() + 1);
            for (PagePathItemUnit pathItemUnit : path)
              resultPath.add(pathItemUnit.pageIndex);

            resultPath.add(bucketIndex);
            return new BucketSearchResult(0, resultPath);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private BucketSearchResult lastItem(OAtomicOperation atomicOperation) throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

    OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else
              return null;
          } else {
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);
              bucketIndex = entry.rightChild;
            } else {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(0);
              bucketIndex = entry.leftChild;
            }

            itemIndex = OSBTreeBucket.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else
              return null;
          } else {
            final ArrayList<Long> resultPath = new ArrayList<Long>(path.size() + 1);
            for (PagePathItemUnit pathItemUnit : path)
              resultPath.add(pathItemUnit.pageIndex);

            resultPath.add(bucketIndex);

            return new BucketSearchResult(bucket.size() - 1, resultPath);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);
        if (itemIndex == OSBTreeBucket.MAX_PAGE_SIZE_BYTES + 1)
          itemIndex = bucket.size() - 1;
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenAscOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      OAtomicOperation atomicOperation) throws IOException {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom, atomicOperation);

    long pageIndexFrom = bucketSearchResultFrom.getLastPathItem();

    int indexFrom;
    if (bucketSearchResultFrom.itemIndex >= 0) {
      indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
    } else {
      indexFrom = -bucketSearchResultFrom.itemIndex - 1;
    }

    return new OSBTreeCursorForward(pageIndexFrom, indexFrom, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenDescOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      OAtomicOperation atomicOperation) throws IOException {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    BucketSearchResult bucketSearchResultTo = findBucket(keyTo, atomicOperation);

    long pageIndexTo = bucketSearchResultTo.getLastPathItem();

    int indexTo;
    if (bucketSearchResultTo.itemIndex >= 0) {
      indexTo = toInclusive ? bucketSearchResultTo.itemIndex : bucketSearchResultTo.itemIndex - 1;
    } else {
      indexTo = -bucketSearchResultTo.itemIndex - 2;
    }

    return new OSBTreeCursorBackward(pageIndexTo, indexTo, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private K enhanceToCompositeKeyBetweenAsc(K keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenAsc(K keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private K enhanceToCompositeKeyBetweenDesc(K keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  private K enhanceFromCompositeKeyBetweenDesc(K keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  private BucketSearchResult splitBucket(List<Long> path, int keyIndex, K keyToInsert, OAtomicOperation atomicOperation)
      throws IOException {
    long pageIndex = path.get(path.size() - 1);

    OCacheEntry bucketEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false);
    try {
      OSBTreeBucket<K, V> bucketToSplit = new OSBTreeBucket<K, V>(bucketEntry, keySerializer, keyTypes, valueSerializer);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBucket.SBTreeEntry<K, V>> rightEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K, V>>(indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (pageIndex != ROOT_INDEX) {
        return splitNonRootBucket(path, keyIndex, keyToInsert, pageIndex, bucketToSplit, splitLeaf, indexToSplit, separationKey,
            rightEntries, atomicOperation);
      } else {
        return splitRootBucket(path, keyIndex, keyToInsert, pageIndex, bucketEntry, bucketToSplit, splitLeaf, indexToSplit,
            separationKey, rightEntries, atomicOperation);
      }
    } finally {
      releasePageFromWrite(atomicOperation, bucketEntry);
    }
  }

  private BucketSearchResult splitNonRootBucket(List<Long> path, int keyIndex, K keyToInsert, long pageIndex,
      OSBTreeBucket<K, V> bucketToSplit, boolean splitLeaf, int indexToSplit, K separationKey,
      List<OSBTreeBucket.SBTreeEntry<K, V>> rightEntries, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rightBucketEntry = addPage(atomicOperation, fileId);

    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer);
      newRightBucket.addAll(rightEntries);

      bucketToSplit.shrink(indexToSplit);

      if (splitLeaf) {
        long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, false);
          OSBTreeBucket<K, V> rightSiblingBucket = new OSBTreeBucket<K, V>(rightSiblingBucketEntry, keySerializer, keyTypes,
              valueSerializer);
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          } finally {
            releasePageFromWrite(atomicOperation, rightSiblingBucketEntry);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false);
      try {
        OSBTreeBucket<K, V> parentBucket = new OSBTreeBucket<K, V>(parentCacheEntry, keySerializer, keyTypes, valueSerializer);
        OSBTreeBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBucket.SBTreeEntry<K, V>(pageIndex,
            rightBucketEntry.getPageIndex(), separationKey, null);

        int insertionIndex = parentBucket.find(separationKey);
        assert insertionIndex < 0;

        insertionIndex = -insertionIndex - 1;
        while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
          releasePageFromWrite(atomicOperation, parentCacheEntry);

          BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey,
              atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false);

          insertionIndex = bucketSearchResult.itemIndex;

          parentBucket = new OSBTreeBucket<K, V>(parentCacheEntry, keySerializer, keyTypes, valueSerializer);
        }

      } finally {
        releasePageFromWrite(atomicOperation, parentCacheEntry);
      }

    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    ArrayList<Long> resultPath = new ArrayList<Long>(path.subList(0, path.size() - 1));

    if (comparator.compare(keyToInsert, separationKey) < 0) {
      resultPath.add(pageIndex);
      return new BucketSearchResult(keyIndex, resultPath);
    }

    resultPath.add(rightBucketEntry.getPageIndex());
    if (splitLeaf) {
      return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
    }

    resultPath.add(rightBucketEntry.getPageIndex());
    return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
  }

  private BucketSearchResult splitRootBucket(List<Long> path, int keyIndex, K keyToInsert, long pageIndex, OCacheEntry bucketEntry,
      OSBTreeBucket<K, V> bucketToSplit, boolean splitLeaf, int indexToSplit, K separationKey,
      List<OSBTreeBucket.SBTreeEntry<K, V>> rightEntries, OAtomicOperation atomicOperation) throws IOException {
    final long freeListPage = bucketToSplit.getValuesFreeListFirstIndex();
    final long treeSize = bucketToSplit.getTreeSize();

    final List<OSBTreeBucket.SBTreeEntry<K, V>> leftEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K, V>>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++)
      leftEntries.add(bucketToSplit.getEntry(i));

    OCacheEntry leftBucketEntry = addPage(atomicOperation, fileId);

    OCacheEntry rightBucketEntry = addPage(atomicOperation, fileId);
    try {
      OSBTreeBucket<K, V> newLeftBucket = new OSBTreeBucket<K, V>(leftBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer);
      newLeftBucket.addAll(leftEntries);

      if (splitLeaf)
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());

    } finally {
      releasePageFromWrite(atomicOperation, leftBucketEntry);
    }

    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer);
      newRightBucket.addAll(rightEntries);

      if (splitLeaf)
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    bucketToSplit = new OSBTreeBucket<K, V>(bucketEntry, false, keySerializer, keyTypes, valueSerializer);

    bucketToSplit.setTreeSize(treeSize);
    bucketToSplit.setValuesFreeListFirstIndex(freeListPage);

    bucketToSplit.addEntry(0,
        new OSBTreeBucket.SBTreeEntry<K, V>(leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), separationKey, null),
        true);

    ArrayList<Long> resultPath = new ArrayList<Long>(path.subList(0, path.size() - 1));

    if (comparator.compare(keyToInsert, separationKey) < 0) {
      resultPath.add(leftBucketEntry.getPageIndex());
      return new BucketSearchResult(keyIndex, resultPath);
    }

    resultPath.add(rightBucketEntry.getPageIndex());

    if (splitLeaf)
      return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

    return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
  }

  private BucketSearchResult findBucket(K key, OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;
    final ArrayList<Long> path = new ArrayList<Long>();

    while (true) {
      if (path.size() > MAX_PATH_LENGTH)
        throw new OSBTreeException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);

      path.add(pageIndex);
      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      final OSBTreeBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(bucketEntry, keySerializer, keyTypes, valueSerializer);
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
        pageIndex = entry.rightChild;
      else
        pageIndex = entry.leftChild;
    }
  }

  private K enhanceCompositeKey(K key, PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey))
      return key;

    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (!(keySize == 1 || compositeKey.getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else
        keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++)
        fullKey.addKey(keyItem);

      return (K) fullKey;
    }

    return key;
  }

  private V readValue(OSBTreeValue<V> sbTreeValue, OAtomicOperation atomicOperation) throws IOException {
    if (!sbTreeValue.isLink())
      return sbTreeValue.getValue();

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, sbTreeValue.getLink(), false);

    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, false);

    int totalSize = valuePage.getSize();
    int currentSize = 0;
    byte[] value = new byte[totalSize];

    while (currentSize < totalSize) {
      currentSize = valuePage.readBinaryContent(value, currentSize);

      long nextPage = valuePage.getNextPage();
      if (nextPage >= 0) {
        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, nextPage, false);

        valuePage = new OSBTreeValuePage(cacheEntry, false);
      }
    }

    releasePageFromRead(atomicOperation, cacheEntry);

    return valueSerializer.deserializeNativeObject(value, 0);
  }

  private Map.Entry<K, V> convertToMapEntry(OSBTreeBucket.SBTreeEntry<K, V> treeEntry, OAtomicOperation atomicOperation)
      throws IOException {
    final K key = treeEntry.key;
    final V value = readValue(treeEntry.value, atomicOperation);

    return new Map.Entry<K, V>() {
      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException("setValue");
      }
    };
  }

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether
   * lowest or highest partially matched key should be used.
   */
  private static enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE, /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  public interface OSBTreeCursor<K, V> {
    Map.Entry<K, V> next(int prefetchSize);
  }

  public interface OSBTreeKeyCursor<K> {
    K next(int prefetchSize);
  }

  private static class BucketSearchResult {
    private final int             itemIndex;
    private final ArrayList<Long> path;

    private BucketSearchResult(int itemIndex, ArrayList<Long> path) {
      this.itemIndex = itemIndex;
      this.path = path;
    }

    public long getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {
    private final long pageIndex;
    private final int  itemIndex;

    private PagePathItemUnit(long pageIndex, int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  public class OSBTreeFullKeyCursor implements OSBTreeKeyCursor<K> {
    private long pageIndex;
    private int  itemIndex;

    private List<K>     keysCache    = new ArrayList<K>();
    private Iterator<K> keysIterator = new OEmptyIterator<K>();

    public OSBTreeFullKeyCursor(long startPageIndex) {
      pageIndex = startPageIndex;
      itemIndex = 0;
    }

    @Override
    public K next(int prefetchSize) {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      startOperation();
      if (statistic != null)
        statistic.startIndexEntryReadTimer();
      try {
        if (keysIterator == null)
          return null;

        if (keysIterator.hasNext())
          return keysIterator.next();

        keysCache.clear();

        if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger())
          prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

        if (prefetchSize == 0)
          prefetchSize = 1;

        atomicOperationsManager.acquireReadLock(OSBTree.this);
        try {
          acquireSharedLock();
          try {
            OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

            while (keysCache.size() < prefetchSize) {
              if (pageIndex == -1)
                break;

              if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
                pageIndex = -1;
                break;
              }

              final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
              try {
                final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);

                if (itemIndex >= bucket.size()) {
                  pageIndex = bucket.getRightSibling();
                  itemIndex = 0;
                  continue;
                }

                final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex), atomicOperation);
                itemIndex++;

                keysCache.add(entry.getKey());
              } finally {
                releasePageFromRead(atomicOperation, cacheEntry);
              }
            }
          } finally {
            releaseSharedLock();
          }
        } catch (IOException e) {
          throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTree.this), e);
        } finally {
          atomicOperationsManager.releaseReadLock(OSBTree.this);
        }

        if (keysCache.isEmpty()) {
          keysCache = null;
          return null;
        }

        keysIterator = keysCache.iterator();
        return keysIterator.next();
      } finally {
        if (statistic != null)
          statistic.stopIndexEntryReadTimer();
        completeOperation();
      }
    }
  }

  private final class OSBTreeCursorForward implements OSBTreeCursor<K, V> {
    private final K       fromKey;
    private final K       toKey;
    private final boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private long pageIndex;
    private int  itemIndex;

    private List<Map.Entry<K, V>>     dataCache         = new ArrayList<Map.Entry<K, V>>();
    private Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorForward(long startPageIndex, int startItemIndex, K fromKey, K toKey, boolean fromKeyInclusive,
        boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      pageIndex = startPageIndex;
      itemIndex = startItemIndex;
    }

    public Map.Entry<K, V> next(int prefetchSize) {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      startOperation();
      if (statistic != null)
        statistic.startIndexEntryReadTimer();
      try {
        if (dataCacheIterator == null)
          return null;

        if (dataCacheIterator.hasNext())
          return dataCacheIterator.next();

        dataCache.clear();

        if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger())
          prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

        if (prefetchSize == 0)
          prefetchSize = 1;

        atomicOperationsManager.acquireReadLock(OSBTree.this);
        try {
          acquireSharedLock();
          try {
            OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

            while (dataCache.size() < prefetchSize) {
              if (pageIndex == -1)
                break;

              if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
                pageIndex = -1;
                break;
              }

              final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
              try {
                final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);

                if (itemIndex >= bucket.size()) {
                  pageIndex = bucket.getRightSibling();
                  itemIndex = 0;
                  continue;
                }

                final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex), atomicOperation);
                itemIndex++;

                if (fromKey != null && (fromKeyInclusive ?
                    comparator.compare(entry.getKey(), fromKey) < 0 :
                    comparator.compare(entry.getKey(), fromKey) <= 0))
                  continue;

                if (toKey != null && (toKeyInclusive ?
                    comparator.compare(entry.getKey(), toKey) > 0 :
                    comparator.compare(entry.getKey(), toKey) >= 0)) {
                  pageIndex = -1;
                  break;
                }

                dataCache.add(entry);
              } finally {
                releasePageFromRead(atomicOperation, cacheEntry);
              }
            }
          } finally {
            releaseSharedLock();
          }
        } catch (IOException e) {
          throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTree.this), e);
        } finally {
          atomicOperationsManager.releaseReadLock(OSBTree.this);
        }

        if (dataCache.isEmpty()) {
          dataCacheIterator = null;
          return null;
        }

        dataCacheIterator = dataCache.iterator();

        return dataCacheIterator.next();
      } finally {
        if (statistic != null)
          statistic.stopIndexEntryReadTimer();
        completeOperation();
      }
    }
  }

  private final class OSBTreeCursorBackward implements OSBTreeCursor<K, V> {
    private final K       fromKey;
    private final K       toKey;
    private final boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private long pageIndex;
    private int  itemIndex;

    private List<Map.Entry<K, V>>     dataCache         = new ArrayList<Map.Entry<K, V>>();
    private Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorBackward(long endPageIndex, int endItemIndex, K fromKey, K toKey, boolean fromKeyInclusive,
        boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      pageIndex = endPageIndex;
      itemIndex = endItemIndex;
    }

    public Map.Entry<K, V> next(int prefetchSize) {
      final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
      startOperation();
      if (statistic != null)
        statistic.startIndexEntryReadTimer();
      try {
        if (dataCacheIterator == null)
          return null;

        if (dataCacheIterator.hasNext())
          return dataCacheIterator.next();

        dataCache.clear();

        if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger())
          prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

        atomicOperationsManager.acquireReadLock(OSBTree.this);
        try {
          acquireSharedLock();
          try {
            OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

            while (dataCache.size() < prefetchSize) {
              if (pageIndex >= getFilledUpTo(atomicOperation, fileId))
                pageIndex = getFilledUpTo(atomicOperation, fileId) - 1;

              if (pageIndex == -1)
                break;

              final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
              try {
                final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer);

                if (itemIndex >= bucket.size())
                  itemIndex = bucket.size() - 1;

                if (itemIndex < 0) {
                  pageIndex = bucket.getLeftSibling();
                  itemIndex = Integer.MAX_VALUE;
                  continue;
                }

                final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex), atomicOperation);
                itemIndex--;

                if (toKey != null && (toKeyInclusive ?
                    comparator.compare(entry.getKey(), toKey) > 0 :
                    comparator.compare(entry.getKey(), toKey) >= 0))
                  continue;

                if (fromKey != null && (fromKeyInclusive ?
                    comparator.compare(entry.getKey(), fromKey) < 0 :
                    comparator.compare(entry.getKey(), fromKey) <= 0)) {
                  pageIndex = -1;
                  break;
                }

                dataCache.add(entry);
              } finally {
                releasePageFromRead(atomicOperation, cacheEntry);
              }
            }
          } finally {
            releaseSharedLock();
          }
        } catch (IOException e) {
          throw OException.wrapException(new OSBTreeException("Error during element iteration", OSBTree.this), e);
        } finally {
          atomicOperationsManager.releaseReadLock(OSBTree.this);
        }

        if (dataCache.isEmpty()) {
          dataCacheIterator = null;
          return null;
        }

        dataCacheIterator = dataCache.iterator();

        return dataCacheIterator.next();
      } finally {
        if (statistic != null)
          statistic.stopIndexEntryReadTimer();
        completeOperation();
      }
    }
  }

  @Override
  protected void startOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic
          .startComponentOperation(getFullName(), OSessionStoragePerformanceStatistic.ComponentType.INDEX);
    }
  }
}
