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
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.OIndexUpdateAction;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.OComponentOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OCreateSBTreeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreeRemoveOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.orientechnologies.orient.core.index.OIndexUpdateAction.changed;

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
public final class OSBTree<K, V> extends ODurableComponent {
  private static final int               MAX_KEY_SIZE            = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final int               MAX_EMBEDDED_VALUE_SIZE = OGlobalConfiguration.SBTREE_MAX_EMBEDDED_VALUE_SIZE
      .getValueAsInteger();
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY         = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY      = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH = OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private final static long                  ROOT_INDEX       = 0;
  private final        Comparator<? super K> comparator       = ODefaultComparator.INSTANCE;
  private final        String                nullFileExtension;
  private              long                  fileId;
  private              long                  nullBucketFileId = -1;
  private              int                   keySize;
  private              OBinarySerializer<K>  keySerializer;
  private              OType[]               keyTypes;
  private              OBinarySerializer<V>  valueSerializer;
  private              boolean               nullPointerSupport;
  private final        AtomicLong            bonsayFileId     = new AtomicLong(0);

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

      fileId = addFile(getFullName());

      if (nullPointerSupport)
        nullBucketFileId = addFile(getName() + nullFileExtension);

      OCacheEntry rootCacheEntry = addPage(fileId);
      try {

        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<>(rootCacheEntry, true, keySerializer, keyTypes, valueSerializer);
        rootBucket.setTreeSize(0);

      } finally {
        releasePageFromWrite(rootCacheEntry, atomicOperation);
      }

      logComponentOperation(atomicOperation, new OCreateSBTreeOperation(atomicOperation.getOperationUnitId(), getName(), fileId));
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
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        checkNullSupport(key);
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          BucketSearchResult bucketSearchResult = findBucket(key);
          if (bucketSearchResult.itemIndex < 0)
            return null;

          long pageIndex = bucketSearchResult.getLastPathItem();
          OCacheEntry keyBucketCacheEntry = loadPageForRead(fileId, pageIndex, false);
          try {
            OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);

            OSBTreeBucket.SBTreeEntry<K, V> treeEntry = keyBucket.getEntry(bucketSearchResult.itemIndex);
            return readValue(treeEntry.value);
          } finally {
            releasePageFromRead(keyBucketCacheEntry);
          }
        } else {
          if (getFilledUpTo(nullBucketFileId) == 0)
            return null;

          final OCacheEntry nullBucketCacheEntry = loadPageForRead(nullBucketFileId, 0, false);
          try {
            final ONullBucket<V> nullBucket = new ONullBucket<>(nullBucketCacheEntry, valueSerializer, false);
            final OSBTreeValue<V> treeValue = nullBucket.getValue();
            if (treeValue == null)
              return null;

            return readValue(treeValue);
          } finally {
            releasePageFromRead(nullBucketCacheEntry);
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
  }

  public void put(K key, V value) {
    put(key, value, null);
  }

  public boolean validatedPut(K key, V value, OIndexEngine.Validator<K, V> validator) {
    return put(key, value, validator);
  }

  private boolean put(K key, V value, OIndexEngine.Validator<K, V> validator) {
    return update(key, (x, bonsayFileId) -> changed(value), validator);
  }

  @SuppressWarnings("unchecked")
  public boolean update(K key, OIndexKeyUpdater<V> updater, OIndexEngine.Validator<K, V> validator) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(true);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during sbtree entrie put", this), e);
    }

    acquireExclusiveLock();
    try {
      OComponentOperation componentOperation = null;
      checkNullSupport(key);

      if (key != null) {
        final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);

        if (keySize > MAX_KEY_SIZE)
          throw new OTooBigIndexKeyException(
              "Key size is more than allowed, operation was canceled. Current key size " + keySize + ", allowed  " + MAX_KEY_SIZE,
              getName());

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        BucketSearchResult bucketSearchResult = findBucket(key);

        OCacheEntry keyBucketCacheEntry = loadPageForWrite(fileId, bucketSearchResult.getLastPathItem(), false);
        OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);

        final byte[] oldRawValue = bucketSearchResult.itemIndex > -1 ? keyBucket.getRawValue(bucketSearchResult.itemIndex) : null;
        final V oldValue;
        if (oldRawValue == null) {
          oldValue = null;
        } else {
          oldValue = valueSerializer.deserializeNativeObject(oldRawValue, 0);
        }

        OIndexUpdateAction<V> updatedValue = updater.update(oldValue, bonsayFileId);
        if (updatedValue.isChange()) {
          V value = updatedValue.getValue();

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
                releasePageFromWrite(keyBucketCacheEntry, atomicOperation);
              }
              if (ignored) // in case of a failure atomic operation will be ended in a usual way below
                endAtomicOperation(false, null);
            }
          }

          final int valueSize = valueSerializer.getObjectSize(value);
          final byte[] serializeValue = new byte[valueSize];
          valueSerializer.serializeNativeObject(value, serializeValue, 0);

          final byte[] serializedKey = new byte[keySize];
          keySerializer.serializeNativeObject(key, serializedKey, 0, (Object[]) keyTypes);

          final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;
          assert !createLinkToTheValue;

          int insertionIndex;
          int sizeDiff;
          if (bucketSearchResult.itemIndex >= 0) {
            assert oldRawValue != null;

            if (oldRawValue.length == serializeValue.length) {
              keyBucket.updateValue(bucketSearchResult.itemIndex, serializeValue);
              releasePageFromWrite(keyBucketCacheEntry, atomicOperation);

              logComponentOperation(atomicOperation,
                  new OSBTreePutOperation(atomicOperation.getOperationUnitId(), getName(), serializedKey, serializeValue,
                      oldRawValue));
              endAtomicOperation(false, null);
              return true;
            } else {
              keyBucket.remove(bucketSearchResult.itemIndex);
              insertionIndex = bucketSearchResult.itemIndex;
              sizeDiff = 0;
            }
          } else {
            insertionIndex = -bucketSearchResult.itemIndex - 1;
            sizeDiff = 1;
          }

          componentOperation = new OSBTreePutOperation(atomicOperation.getOperationUnitId(), getName(), serializedKey,
              serializeValue, oldRawValue);
          while (!keyBucket.addLeafEntry(insertionIndex, serializedKey, serializeValue)) {
            releasePageFromWrite(keyBucketCacheEntry, atomicOperation);

            bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key, atomicOperation);

            insertionIndex = bucketSearchResult.itemIndex;

            keyBucketCacheEntry = loadPageForWrite(fileId, bucketSearchResult.getLastPathItem(), false);

            keyBucket = new OSBTreeBucket<>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);
          }

          releasePageFromWrite(keyBucketCacheEntry, atomicOperation);

          if (sizeDiff != 0) {
            updateSize(sizeDiff, atomicOperation);
          }
        } else if (updatedValue.isRemove()) {
          final byte[] serializedKey = new byte[keySize];
          keySerializer.serializeNativeObject(key, serializedKey, 0, (Object[]) keyTypes);

          componentOperation = new OSBTreeRemoveOperation(atomicOperation.getOperationUnitId(), getName(), serializedKey,
              oldRawValue);

          removeKey(bucketSearchResult, atomicOperation);
          releasePageFromWrite(keyBucketCacheEntry, atomicOperation);
        } else if (updatedValue.isNothing()) {
          releasePageFromWrite(keyBucketCacheEntry, atomicOperation);
        }
      } else {
        boolean needNew = false;

        if (getFilledUpTo(nullBucketFileId) == 0) {
          needNew = true;
        }

        int sizeDiff = 0;

        OCacheEntry cacheEntry = null;
        boolean ignored = false;
        try {
          if (!needNew) {
            cacheEntry = loadPageForWrite(nullBucketFileId, 0, false);

            final ONullBucket<V> nullBucket = new ONullBucket<>(cacheEntry, valueSerializer, false);
            final byte[] oldRawValue = nullBucket.getRawValue();
            final V oldValue;
            if (oldRawValue == null) {
              oldValue = null;
            } else {
              oldValue = valueSerializer.deserializeNativeObject(oldRawValue, 0);
            }

            final OIndexUpdateAction<V> updatedValue = updater.update(oldValue, bonsayFileId);
            if (updatedValue.isChange()) {
              V value = updatedValue.getValue();
              if (validator != null) {
                final Object result = validator.validate(null, oldValue, value);
                if (result == OIndexEngine.Validator.IGNORE) {
                  ignored = true;
                  return false;
                }

                value = (V) result;
              }

              if (oldValue != null) {
                sizeDiff = -1;
              }

              final int valueSize = valueSerializer.getObjectSize(value);
              final byte[] rawValue = new byte[valueSize];
              valueSerializer.serializeNativeObject(value, rawValue, 0);

              componentOperation = new OSBTreePutOperation(atomicOperation.getOperationUnitId(), getName(), null, rawValue,
                  oldRawValue);

              nullBucket.setRawValue(rawValue);
            } else if (updatedValue.isRemove()) {
              componentOperation = new OSBTreeRemoveOperation(atomicOperation.getOperationUnitId(), getName(), null, oldRawValue);
              removeNullBucket(atomicOperation);
            } else if (updatedValue.isNothing()) {
              //Do Nothing
            }
          } else {
            cacheEntry = addPage(nullBucketFileId);

            final ONullBucket<V> nullBucket = new ONullBucket<>(cacheEntry, valueSerializer, true);

            final OIndexUpdateAction<V> updatedValue = updater.update(null, bonsayFileId);
            if (updatedValue.isChange()) {
              V value = updatedValue.getValue();
              if (validator != null) {
                final Object result = validator.validate(null, null, value);
                if (result == OIndexEngine.Validator.IGNORE) {
                  ignored = true;
                  return false;
                }

                value = (V) result;
              }

              final int valueSize = valueSerializer.getObjectSize(value);
              final byte[] rawValue = new byte[valueSize];
              valueSerializer.serializeNativeObject(value, rawValue, 0);

              componentOperation = new OSBTreePutOperation(atomicOperation.getOperationUnitId(), getName(), null, rawValue, null);

              nullBucket.setRawValue(rawValue);
            } else if (updatedValue.isRemove()) {
              componentOperation = new OSBTreeRemoveOperation(atomicOperation.getOperationUnitId(), getName(), null, null);
              removeNullBucket(atomicOperation);
            } else if (updatedValue.isNothing()) {
              //Do Nothing
            }
          }
        } finally {
          assert cacheEntry != null;

          releasePageFromWrite(cacheEntry, atomicOperation);
          if (ignored) {
            endAtomicOperation(false, null);
          }
        }

        sizeDiff++;
        updateSize(sizeDiff, atomicOperation);
      }

      if (componentOperation != null) {
        logComponentOperation(atomicOperation, componentOperation);
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
  }

  public void close(boolean flush) {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, flush, writeCache);

      if (nullPointerSupport)
        readCache.closeFile(nullBucketFileId, flush, writeCache);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void close() {
    close(true);
  }

  public void clear() {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(true);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during sbtree clear", this), e);
    }

    acquireExclusiveLock();
    try {
      truncateFile(fileId);

      if (nullPointerSupport)
        truncateFile(nullBucketFileId);

      OCacheEntry cacheEntry = loadPageForWrite(fileId, ROOT_INDEX, false);
      if (cacheEntry == null) {
        cacheEntry = addPage(fileId);
      }

      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<>(cacheEntry, true, keySerializer, keyTypes, valueSerializer);

        rootBucket.setTreeSize(0);

      } finally {
        releasePageFromWrite(cacheEntry, atomicOperation);
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
  }

  public void delete() {
    try {
      startAtomicOperation(false);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during sbtree deletion", this), e);
    }

    acquireExclusiveLock();
    try {
      deleteFile(fileId);

      if (nullPointerSupport)
        deleteFile(nullBucketFileId);

      endAtomicOperation(false, null);
    } catch (Exception e) {
      rollback(e);
      throw OException.wrapException(new OSBTreeException("Error during delete of sbtree with name " + getName(), this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void deleteWithoutLoad() {
    try {
      startAtomicOperation(false);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during sbtree deletion", this), e);
    }

    acquireExclusiveLock();
    try {
      if (isFileExists(getFullName())) {
        final long fileId = openFile(getFullName());
        deleteFile(fileId);
      }

      if (isFileExists(getName() + nullFileExtension)) {
        final long nullFileId = openFile(getName() + nullFileExtension);
        deleteFile(nullFileId);
      }

      endAtomicOperation(false, null);
    } catch (Exception e) {
      rollback(e);
      throw OException.wrapException(new OSBTreeException("Exception during deletion of sbtree " + getName(), this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      int keySize, boolean nullPointerSupport) {
    acquireExclusiveLock();
    try {
      this.keySize = keySize;
      if (keyTypes != null)
        this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
      else
        this.keyTypes = null;

      this.nullPointerSupport = nullPointerSupport;

      fileId = openFile(getFullName());
      if (nullPointerSupport)
        nullBucketFileId = openFile(name + nullFileExtension);

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Exception during loading of sbtree " + name, this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OCacheEntry rootCacheEntry = loadPageForRead(fileId, ROOT_INDEX, false);
        try {
          OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
          return rootBucket.getTreeSize();
        } finally {
          releasePageFromRead(rootCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during retrieving of size of index " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public V remove(K key) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(true);
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during sbtree entrie remove", this), e);
    }

    acquireExclusiveLock();
    try {
      byte[] removedValue;

      if (key != null) {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        BucketSearchResult bucketSearchResult = findBucket(key);
        if (bucketSearchResult.itemIndex < 0) {
          endAtomicOperation(false, null);
          return null;
        }

        removedValue = removeKey(bucketSearchResult, atomicOperation);
      } else {
        if (getFilledUpTo(nullBucketFileId) == 0) {
          endAtomicOperation(false, null);
          return null;
        }

        removedValue = removeNullBucket(atomicOperation);
      }

      if (removedValue != null) {
        final byte[] rawKey;

        if (key != null) {
          final int keyLen = keySerializer.getObjectSize(key, (Object[]) keyTypes);
          rawKey = new byte[keyLen];
          keySerializer.serializeNativeObject(key, rawKey, 0, (Object[]) keyTypes);
        } else {
          rawKey = null;
        }

        logComponentOperation(atomicOperation,
            new OSBTreeRemoveOperation(atomicOperation.getOperationUnitId(), getName(), rawKey, removedValue));
      }

      endAtomicOperation(false, null);

      if (removedValue == null) {
        return null;
      }

      return valueSerializer.deserializeNativeObject(removedValue, 0);
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
  }

  private byte[] removeNullBucket(OAtomicOperation atomicOperation) throws IOException {
    byte[] removedValue;
    OCacheEntry nullCacheEntry = loadPageForWrite(nullBucketFileId, 0, false);
    try {
      ONullBucket<V> nullBucket = new ONullBucket<>(nullCacheEntry, valueSerializer, false);
      removedValue = nullBucket.getRawValue();

      if (removedValue != null) {
        nullBucket.removeValue();
      }

    } finally {
      releasePageFromWrite(nullCacheEntry, atomicOperation);
    }

    if (removedValue != null) {
      updateSize(-1, atomicOperation);
    }

    return removedValue;
  }

  private byte[] removeKey(BucketSearchResult bucketSearchResult, OAtomicOperation atomicOperation) throws IOException {
    byte[] removedValue;
    OCacheEntry keyBucketCacheEntry = loadPageForWrite(fileId, bucketSearchResult.getLastPathItem(), false);
    try {
      OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer);

      removedValue = keyBucket.getRawValue(bucketSearchResult.itemIndex);
      keyBucket.remove(bucketSearchResult.itemIndex);
      updateSize(-1, atomicOperation);
    } finally {
      releasePageFromWrite(keyBucketCacheEntry, atomicOperation);
    }
    return removedValue;
  }

  public OSBTreeCursor<K, V> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder) {

    if (!ascSortOrder)
      return iterateEntriesMinorDesc(key, inclusive);

    return iterateEntriesMinorAsc(key, inclusive);
  }

  public OSBTreeCursor<K, V> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder) {
    if (ascSortOrder)
      return iterateEntriesMajorAsc(key, inclusive);

    return iterateEntriesMajorDesc(key, inclusive);
  }

  public K firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final BucketSearchResult searchResult = firstItem();
        if (searchResult == null)
          return null;

        final OCacheEntry cacheEntry = loadPageForRead(fileId, searchResult.getLastPathItem(), false);
        try {
          OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);
          return bucket.getKey(searchResult.itemIndex);
        } finally {
          releasePageFromRead(cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public K lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final BucketSearchResult searchResult = lastItem();
        if (searchResult == null)
          return null;

        final OCacheEntry cacheEntry = loadPageForRead(fileId, searchResult.getLastPathItem(), false);
        try {
          OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);
          return bucket.getKey(searchResult.itemIndex);
        } finally {
          releasePageFromRead(cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding last key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }

  }

  public OSBTreeKeyCursor<K> keyCursor() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final BucketSearchResult searchResult = firstItem();
        if (searchResult == null)
          return prefetchSize -> null;

        return new OSBTreeFullKeyCursor(searchResult.getLastPathItem());
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OSBTreeException("Error during finding first key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      boolean ascSortOrder) {
    if (ascSortOrder)
      return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive);
    else
      return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive);
  }

  public void flush() {
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

  private void rollback(Exception e) {
    try {
      endAtomicOperation(true, e);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  private void updateSize(long diffSize, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rootCacheEntry = loadPageForWrite(fileId, ROOT_INDEX, false);
    try {
      OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<>(rootCacheEntry, keySerializer, keyTypes, valueSerializer);
      rootBucket.setTreeSize(rootBucket.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(rootCacheEntry, atomicOperation);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorDesc(K key, boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new OSBTreeCursorBackward(null, key, false, inclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorAsc(K key, boolean inclusive) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMinorAsc(key, inclusive);

      return new OSBTreeCursorForward(null, key, false, inclusive);
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

  private OSBTreeCursor<K, V> iterateEntriesMajorAsc(K key, boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new OSBTreeCursorForward(key, null, inclusive, false);
  }

  private OSBTreeCursor<K, V> iterateEntriesMajorDesc(K key, boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorDesc(key, inclusive);

    return new OSBTreeCursorBackward(key, null, inclusive, false);
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

  private BucketSearchResult firstItem() throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(fileId, bucketIndex, false);
    int itemIndex = 0;
    try {
      OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);

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
            final ArrayList<Long> resultPath = new ArrayList<>(path.size() + 1);
            for (PagePathItemUnit pathItemUnit : path)
              resultPath.add(pathItemUnit.pageIndex);

            resultPath.add(bucketIndex);
            return new BucketSearchResult(0, resultPath);
          }
        }

        releasePageFromRead(cacheEntry);

        cacheEntry = loadPageForRead(fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);
      }
    } finally {
      releasePageFromRead(cacheEntry);
    }
  }

  private BucketSearchResult lastItem() throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(fileId, bucketIndex, false);

    OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);

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
            final ArrayList<Long> resultPath = new ArrayList<>(path.size() + 1);
            for (PagePathItemUnit pathItemUnit : path)
              resultPath.add(pathItemUnit.pageIndex);

            resultPath.add(bucketIndex);

            return new BucketSearchResult(bucket.size() - 1, resultPath);
          }
        }

        releasePageFromRead(cacheEntry);

        cacheEntry = loadPageForRead(fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);
        if (itemIndex == OSBTreeBucket.MAX_PAGE_SIZE_BYTES + 1)
          itemIndex = bucket.size() - 1;
      }
    } finally {
      releasePageFromRead(cacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenAscOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new OSBTreeCursorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenDescOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new OSBTreeCursorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
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

    OCacheEntry bucketEntry = loadPageForWrite(fileId, pageIndex, false);
    try {
      OSBTreeBucket<K, V> bucketToSplit = new OSBTreeBucket<>(bucketEntry, keySerializer, keyTypes, valueSerializer);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++) {
        rightEntries.add(bucketToSplit.getRawEntry(i));
      }

      if (pageIndex != ROOT_INDEX) {
        return splitNonRootBucket(path, keyIndex, keyToInsert, pageIndex, bucketToSplit, splitLeaf, indexToSplit, separationKey,
            rightEntries, atomicOperation);
      } else {
        return splitRootBucket(path, keyIndex, keyToInsert, bucketEntry, bucketToSplit, splitLeaf, indexToSplit, separationKey,
            rightEntries, atomicOperation);
      }
    } finally {
      releasePageFromWrite(bucketEntry, atomicOperation);
    }
  }

  private BucketSearchResult splitNonRootBucket(List<Long> path, int keyIndex, K keyToInsert, long pageIndex,
      OSBTreeBucket<K, V> bucketToSplit, boolean splitLeaf, int indexToSplit, K separationKey, List<byte[]> rightEntries,
      OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry rightBucketEntry = addPage(fileId);

    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer);
      newRightBucket.addAll(rightEntries);

      bucketToSplit.shrink(indexToSplit);

      if (splitLeaf) {
        long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(fileId, rightSiblingPageIndex, false);
          OSBTreeBucket<K, V> rightSiblingBucket = new OSBTreeBucket<>(rightSiblingBucketEntry, keySerializer, keyTypes,
              valueSerializer);
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          } finally {
            releasePageFromWrite(rightSiblingBucketEntry, atomicOperation);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(fileId, parentIndex, false);
      try {
        OSBTreeBucket<K, V> parentBucket = new OSBTreeBucket<>(parentCacheEntry, keySerializer, keyTypes, valueSerializer);
        OSBTreeBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBucket.SBTreeEntry<>(pageIndex, rightBucketEntry.getPageIndex(),
            separationKey, null);

        int insertionIndex = parentBucket.find(separationKey);
        assert insertionIndex < 0;

        insertionIndex = -insertionIndex - 1;
        while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
          releasePageFromWrite(parentCacheEntry, atomicOperation);

          BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey,
              atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          parentCacheEntry = loadPageForWrite(fileId, parentIndex, false);

          insertionIndex = bucketSearchResult.itemIndex;

          parentBucket = new OSBTreeBucket<>(parentCacheEntry, keySerializer, keyTypes, valueSerializer);
        }

      } finally {
        releasePageFromWrite(parentCacheEntry, atomicOperation);
      }

    } finally {
      releasePageFromWrite(rightBucketEntry, atomicOperation);
    }

    ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));

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

  private BucketSearchResult splitRootBucket(List<Long> path, int keyIndex, K keyToInsert, OCacheEntry bucketEntry,
      OSBTreeBucket<K, V> bucketToSplit, boolean splitLeaf, int indexToSplit, K separationKey, List<byte[]> rightEntries,
      OAtomicOperation atomicOperation) throws IOException {
    final long freeListPage = bucketToSplit.getValuesFreeListFirstIndex();
    final long treeSize = bucketToSplit.getTreeSize();

    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++)
      leftEntries.add(bucketToSplit.getRawEntry(i));

    OCacheEntry leftBucketEntry = addPage(fileId);

    OCacheEntry rightBucketEntry = addPage(fileId);
    try {
      OSBTreeBucket<K, V> newLeftBucket = new OSBTreeBucket<>(leftBucketEntry, splitLeaf, keySerializer, keyTypes, valueSerializer);
      newLeftBucket.addAll(leftEntries);

      if (splitLeaf)
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());

    } finally {
      releasePageFromWrite(leftBucketEntry, atomicOperation);
    }

    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer);
      newRightBucket.addAll(rightEntries);

      if (splitLeaf)
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
    } finally {
      releasePageFromWrite(rightBucketEntry, atomicOperation);
    }

    bucketToSplit = new OSBTreeBucket<>(bucketEntry, false, keySerializer, keyTypes, valueSerializer);

    bucketToSplit.setTreeSize(treeSize);
    bucketToSplit.setValuesFreeListFirstIndex(freeListPage);

    bucketToSplit.addEntry(0,
        new OSBTreeBucket.SBTreeEntry<>(leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), separationKey, null),
        true);

    ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));

    if (comparator.compare(keyToInsert, separationKey) < 0) {
      resultPath.add(leftBucketEntry.getPageIndex());
      return new BucketSearchResult(keyIndex, resultPath);
    }

    resultPath.add(rightBucketEntry.getPageIndex());

    if (splitLeaf)
      return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

    return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
  }

  private BucketSearchResult findBucket(K key) throws IOException {
    long pageIndex = ROOT_INDEX;
    final ArrayList<Long> path = new ArrayList<>();

    while (true) {
      if (path.size() > MAX_PATH_LENGTH)
        throw new OSBTreeException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);

      path.add(pageIndex);
      final OCacheEntry bucketEntry = loadPageForRead(fileId, pageIndex, false);
      final OSBTreeBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<>(bucketEntry, keySerializer, keyTypes, valueSerializer);
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
        releasePageFromRead(bucketEntry);
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

      //noinspection unchecked
      return (K) fullKey;
    }

    return key;
  }

  private V readValue(OSBTreeValue<V> sbTreeValue) throws IOException {
    if (!sbTreeValue.isLink())
      return sbTreeValue.getValue();

    OCacheEntry cacheEntry = loadPageForRead(fileId, sbTreeValue.getLink(), false);

    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, false);

    int totalSize = valuePage.getSize();
    int currentSize = 0;
    byte[] value = new byte[totalSize];

    while (currentSize < totalSize) {
      currentSize = valuePage.readBinaryContent(value, currentSize);

      long nextPage = valuePage.getNextPage();
      if (nextPage >= 0) {
        releasePageFromRead(cacheEntry);

        cacheEntry = loadPageForRead(fileId, nextPage, false);

        valuePage = new OSBTreeValuePage(cacheEntry, false);
      }
    }

    releasePageFromRead(cacheEntry);

    return valueSerializer.deserializeNativeObject(value, 0);
  }

  private Map.Entry<K, V> convertToMapEntry(OSBTreeBucket.SBTreeEntry<K, V> treeEntry) throws IOException {
    final K key = treeEntry.key;
    final V value = readValue(treeEntry.value);

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
  private enum PartialSearchMode {
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

    long getLastPathItem() {
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

    private List<K>     keysCache    = new ArrayList<>();
    private Iterator<K> keysIterator = new OEmptyIterator<>();

    OSBTreeFullKeyCursor(long startPageIndex) {
      pageIndex = startPageIndex;
      itemIndex = 0;
    }

    @Override
    public K next(int prefetchSize) {
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
          while (keysCache.size() < prefetchSize) {
            if (pageIndex == -1)
              break;

            if (pageIndex >= getFilledUpTo(fileId)) {
              pageIndex = -1;
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(fileId, pageIndex, false);
            try {
              final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);

              if (itemIndex >= bucket.size()) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
              itemIndex++;

              keysCache.add(entry.getKey());
            } finally {
              releasePageFromRead(cacheEntry);
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
    }
  }

  private final class OSBTreeCursorForward implements OSBTreeCursor<K, V> {
    private       K       fromKey;
    private final K       toKey;
    private       boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private List<Map.Entry<K, V>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorForward(K fromKey, K toKey, boolean fromKeyInclusive, boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (fromKey == null) {
        this.fromKeyInclusive = true;
      }
    }

    public Map.Entry<K, V> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, V> entry = dataCacheIterator.next();

        fromKey = entry.getKey();
        fromKeyInclusive = false;

        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      if (prefetchSize == 0) {
        prefetchSize = 1;
      }

      atomicOperationsManager.acquireReadLock(OSBTree.this);
      try {
        acquireSharedLock();
        try {
          final BucketSearchResult bucketSearchResult;

          if (fromKey != null) {
            bucketSearchResult = findBucket(fromKey);
          } else {
            bucketSearchResult = firstItem();
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.getLastPathItem();
          int itemIndex;

          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = fromKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 1;
          }

          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(fileId, pageIndex, false);
            try {
              final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);

              if (itemIndex >= bucket.size()) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
              itemIndex++;

              if (fromKey != null && (fromKeyInclusive ?
                  comparator.compare(entry.getKey(), fromKey) < 0 :
                  comparator.compare(entry.getKey(), fromKey) <= 0))
                continue;

              if (toKey != null && (toKeyInclusive ?
                  comparator.compare(entry.getKey(), toKey) > 0 :
                  comparator.compare(entry.getKey(), toKey) >= 0)) {
                break;
              }

              dataCache.add(entry);
            } finally {
              releasePageFromRead(cacheEntry);
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

      final Map.Entry<K, V> entry = dataCacheIterator.next();

      fromKey = entry.getKey();
      fromKeyInclusive = false;

      return entry;
    }
  }

  private final class OSBTreeCursorBackward implements OSBTreeCursor<K, V> {
    private final K       fromKey;
    private       K       toKey;
    private final boolean fromKeyInclusive;
    private       boolean toKeyInclusive;

    private List<Map.Entry<K, V>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private Iterator<Map.Entry<K, V>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OSBTreeCursorBackward(K fromKey, K toKey, boolean fromKeyInclusive, boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (toKey == null) {
        this.toKeyInclusive = true;
      }
    }

    public Map.Entry<K, V> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, V> entry = dataCacheIterator.next();
        toKey = entry.getKey();

        toKeyInclusive = false;
        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      atomicOperationsManager.acquireReadLock(OSBTree.this);
      try {
        acquireSharedLock();
        try {
          final BucketSearchResult bucketSearchResult;

          if (toKey != null) {
            bucketSearchResult = findBucket(toKey);
          } else {
            bucketSearchResult = lastItem();
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.getLastPathItem();

          int itemIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = toKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 2;
          }

          while (dataCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(fileId, pageIndex, false);
            try {
              final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<>(cacheEntry, keySerializer, keyTypes, valueSerializer);

              if (itemIndex >= bucket.size()) {
                itemIndex = bucket.size() - 1;
              }

              if (itemIndex < 0) {
                pageIndex = bucket.getLeftSibling();
                itemIndex = Integer.MAX_VALUE;
                continue;
              }

              final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
              itemIndex--;

              if (toKey != null && (toKeyInclusive ?
                  comparator.compare(entry.getKey(), toKey) > 0 :
                  comparator.compare(entry.getKey(), toKey) >= 0))
                continue;

              if (fromKey != null && (fromKeyInclusive ?
                  comparator.compare(entry.getKey(), fromKey) < 0 :
                  comparator.compare(entry.getKey(), fromKey) <= 0)) {
                break;
              }

              dataCache.add(entry);
            } finally {
              releasePageFromRead(cacheEntry);
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

      final Map.Entry<K, V> entry = dataCacheIterator.next();

      toKey = entry.getKey();
      toKeyInclusive = false;

      return entry;
    }
  }
}
