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

package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.OCellBTreeMultiValue;

import java.io.IOException;
import java.util.*;

/**
 * This is implementation which is based on B+-tree implementation threaded tree. The main differences are:
 * <ol>
 * <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused later when new items are added.</li>
 * <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more than keys contained in current
 * bucket</li> <ol/> There is support of null values for keys, but values itself cannot be null. Null keys support is switched off
 * by default if null keys are supported value which is related to null key will be stored in separate file which has only one page.
 * Buckets/pages for usual (non-null) key-value entries can be considered as sorted array. The first bytes of page contains such
 * auxiliary information as size of entries contained in bucket, links to neighbors which contain entries with keys less/more than
 * keys in current bucket. The next bytes contain sorted array of entries. Array itself is split on two parts. First part is growing
 * from start to end, and second part is growing from end to start. First part is array of offsets to real key-value entries which
 * are stored in second part of array which grows from end to start. This array of offsets is sorted by accessing order according to
 * key value. So we can use binary search to find requested key. When new key-value pair is added we append binary presentation of
 * this pair to the second part of array which grows from end of page to start, remember value of offset for this pair, and find
 * proper position of this offset inside of first part of array. Such approach allows to minimize amount of memory involved in
 * performing of operations and as result speed up data processing.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public final class OCellBTreeMultiValueV3<K> extends ODurableComponent implements OCellBTreeMultiValue<K> {
  private static final int               M_ID_BATCH_SIZE    = 131_072;
  private static final int               MAX_KEY_SIZE       = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private static final int MAX_PATH_LENGTH = OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final int  ENTRY_POINT_INDEX = 0;
  private static final long ROOT_INDEX        = 1;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;
  private final String                nullFileExtension;
  private final String                containerExtension;

  private long fileId;
  private long nullBucketFileId;

  private int                  keySize;
  private OBinarySerializer<K> keySerializer;
  private OType[]              keyTypes;

  private       OSBTreeV1<OMultiValueEntry, Byte> multiContainer;
  private final OModifiableLong                   mIdCounter = new OModifiableLong();

  public OCellBTreeMultiValueV3(final String name, final String dataFileExtension, final String nullFileExtension,
      final String containerExtension, final OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
      this.containerExtension = containerExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(final OBinarySerializer<K> keySerializer, final OType[] keyTypes, final int keySize,
      final OEncryption encryption) throws IOException {
    assert keySerializer != null;

    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
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

        final OCacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId);
        try {
          final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
          entryPoint.init();
        } finally {
          releasePageFromWrite(atomicOperation, entryPointCacheEntry);
        }

        final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId);
        try {
          @SuppressWarnings("unused")
          final Bucket<K> rootBucket = new Bucket<>(rootCacheEntry, true, keySerializer, multiContainer);
        } finally {
          releasePageFromWrite(atomicOperation, rootCacheEntry);
        }

        final OCacheEntry nullBucketEntry = addPage(atomicOperation, nullBucketFileId);
        try {
          final ONullBucket nullBucket = new ONullBucket(nullBucketEntry, multiContainer);
          nullBucket.init(incrementMId(atomicOperation));
        } finally {
          releasePageFromWrite(atomicOperation, nullBucketEntry);
        }

        multiContainer = new OSBTreeV1<>(getName(), containerExtension, null, storage);
        multiContainer.create(MultiValueEntrySerializer.INSTANCE, OByteSerializer.INSTANCE, null, 1, false, null);
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }

  }

  public List<ORID> get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return Collections.emptyList();
          }

          final long pageIndex = bucketSearchResult.pageIndex;
          final int itemIndex = bucketSearchResult.itemIndex;

          long leftSibling = -1;
          long rightSibling = -1;

          final List<ORID> result = new ArrayList<>(8);
          OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
          try {
            final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
            result.addAll(bucket.getValues(itemIndex));

            if (itemIndex == 0) {
              leftSibling = bucket.getLeftSibling();
            }

            if (itemIndex == bucket.size() - 1) {
              rightSibling = bucket.getRightSibling();
            }
          } finally {
            releasePageFromRead(atomicOperation, cacheEntry);
          }

          while (leftSibling >= 0) {
            cacheEntry = loadPageForRead(atomicOperation, fileId, leftSibling, false);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.getKey(size - 1).equals(key)) {
                  result.addAll(bucket.getValues(size - 1));

                  if (size == 1) {
                    leftSibling = bucket.getLeftSibling();
                  } else {
                    leftSibling = -1;
                  }
                } else {
                  leftSibling = -1;
                }
              } else {
                leftSibling = bucket.getLeftSibling();
              }
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }

          while (rightSibling >= 0) {
            cacheEntry = loadPageForRead(atomicOperation, fileId, rightSibling, false);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.getKey(0).equals(key)) {
                  result.addAll(bucket.getValues(0));

                  if (size == 1) {
                    rightSibling = bucket.getRightSibling();
                  } else {
                    rightSibling = -1;
                  }
                } else {
                  rightSibling = -1;
                }
              } else {
                rightSibling = bucket.getRightSibling();
              }
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }

          return result;
        } else {
          final OCacheEntry nullCacheEntry = loadPageForRead(atomicOperation, nullBucketFileId, 0, false);
          try {
            final ONullBucket nullBucket = new ONullBucket(nullCacheEntry, multiContainer);
            return nullBucket.getValues();
          } finally {
            releasePageFromRead(atomicOperation, nullCacheEntry);
          }
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeMultiValueException("Error during retrieving  of sbtree with name " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void put(K key, final ORID value) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        if (key != null) {

          key = keySerializer.preprocess(key, (Object[]) keyTypes);
          final byte[] serializedKey = keySerializer.serializeNativeAsWhole(key, (Object[]) keyTypes);

          if (keySize > MAX_KEY_SIZE) {
            throw new OTooBigIndexKeyException(
                "Key size is more than allowed, operation was canceled. Current key size " + keySize + ", allowed  " + MAX_KEY_SIZE,
                getName());
          }

          UpdateBucketSearchResult bucketSearchResult = findBucketForUpdate(key, atomicOperation);

          OCacheEntry keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false,
              true);
          Bucket<K> keyBucket = new Bucket<>(keyBucketCacheEntry, keySerializer, multiContainer);

          final byte[] keyToInsert;
          final byte[] keyToInsert1;

          keyToInsert1 = serializedKey;

          keyToInsert = keyToInsert1;

          final boolean isNew;
          int insertionIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            insertionIndex = bucketSearchResult.itemIndex;
            isNew = false;
          } else {
            insertionIndex = -bucketSearchResult.itemIndex - 1;
            isNew = true;
          }

          while (!addEntry(keyBucket, insertionIndex, isNew, keyToInsert, value, atomicOperation)) {
            bucketSearchResult = splitBucket(keyBucket, keyBucketCacheEntry, bucketSearchResult.path,
                bucketSearchResult.insertionIndexes, insertionIndex, key, atomicOperation);

            insertionIndex = bucketSearchResult.itemIndex;

            final long pageIndex = bucketSearchResult.getLastPathItem();

            if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
              releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

              keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            }

            keyBucket = new Bucket<>(keyBucketCacheEntry, keySerializer, multiContainer);
          }

          releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

          updateSize(1, atomicOperation);
        } else {
          final OCacheEntry nullCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);

          try {
            final ONullBucket nullBucket = new ONullBucket(nullCacheEntry, multiContainer);
            nullBucket.addValue(value);
          } finally {
            releasePageFromWrite(atomicOperation, nullCacheEntry);
          }

          updateSize(1, atomicOperation);
        }
      } finally {
        releaseExclusiveLock();
      }
    } catch (final RuntimeException | IOException e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  private boolean addEntry(final Bucket<K> bucketMultiValue, final int index, final boolean isNew, final byte[] key,
      final ORID value, final OAtomicOperation atomicOperation) throws IOException {

    final long newCounter = incrementMId(atomicOperation);
    if (isNew) {
      return bucketMultiValue.addNewLeafEntry(index, key, value, newCounter);
    }

    return bucketMultiValue.appendNewLeafEntry(index, value);
  }

  private long incrementMId(final OAtomicOperation atomicOperation) throws IOException {
    final long idCounter = mIdCounter.getValue();

    if ((idCounter & (M_ID_BATCH_SIZE - 1)) == 0) {
      final OCacheEntry cacheEntryPoint = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
      try {
        final OEntryPoint<K> entryPoint = new OEntryPoint<>(cacheEntryPoint);
        entryPoint.setEntryId(idCounter + M_ID_BATCH_SIZE);
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntryPoint);
      }
    }

    mIdCounter.setValue(idCounter + 1);

    return idCounter + 1;
  }

  public void close() {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, true, writeCache);
      readCache.closeFile(nullBucketFileId, true, writeCache);
      multiContainer.close();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void clear() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
        try {
          final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
          entryPoint.init();
        } finally {
          releasePageFromWrite(atomicOperation, entryPointCacheEntry);
        }

        final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, ROOT_INDEX, false, true);
        try {
          @SuppressWarnings("unused")
          final Bucket<K> rootBucket = new Bucket<>(cacheEntry, true, keySerializer, multiContainer);
        } finally {
          releasePageFromWrite(atomicOperation, cacheEntry);
        }

        final OCacheEntry nullBucketCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
        try {
          final ONullBucket nullBucket = new ONullBucket(nullBucketCacheEntry, multiContainer);
          nullBucket.clear();
        } finally {
          releasePageFromWrite(atomicOperation, nullBucketCacheEntry);
        }

        multiContainer.clear();
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  public void delete() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);
        deleteFile(atomicOperation, nullBucketFileId);

        multiContainer.delete();
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  public void deleteWithoutLoad() throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
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
      } finally {
        releaseExclusiveLock();
      }
    } catch (final Exception e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  public void load(final String name, final int keySize, final OType[] keyTypes, final OBinarySerializer<K> keySerializer,
      final OEncryption encryption) {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
      nullBucketFileId = openFile(atomicOperation, name + nullFileExtension);

      this.keySize = keySize;
      this.keyTypes = keyTypes;
      this.keySerializer = keySerializer;

      multiContainer = new OSBTreeV1<>(getName(), containerExtension, null, storage);
      multiContainer.load(getName(), MultiValueEntrySerializer.INSTANCE, OByteSerializer.INSTANCE, null, 1, false, null);

      final OCacheEntry entryPointCacheEntry = loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
      try {
        final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
        mIdCounter.setValue(entryPoint.getEntryId());
      } finally {
        releasePageFromRead(atomicOperation, entryPointCacheEntry);
      }

    } catch (final IOException e) {
      throw OException.wrapException(new OCellBTreeMultiValueException("Exception during loading of sbtree " + name, this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final OCacheEntry entryPointCacheEntry = loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
        try {
          final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
          return entryPoint.getTreeSize();
        } finally {
          releasePageFromRead(atomicOperation, entryPointCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeMultiValueException("Error during retrieving of size of index " + getName(), this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public boolean remove(K key) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);
    try {
      acquireExclusiveLock();
      try {
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return false;
          }

          final long pageIndex = bucketSearchResult.pageIndex;
          final int itemIndex = bucketSearchResult.itemIndex;

          long leftSibling = -1;
          long rightSibling = -1;

          int removed;
          OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
          try {
            final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
            removed = bucket.remove(itemIndex);

            if (itemIndex == 0) {
              leftSibling = bucket.getLeftSibling();
            }

            if (itemIndex == bucket.size() - 1) {
              rightSibling = bucket.getRightSibling();
            }
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          while (leftSibling >= 0) {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, leftSibling, false, true);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.getKey(size - 1).equals(key)) {
                  removed += bucket.remove(size - 1);

                  if (size <= 1) {
                    leftSibling = bucket.getLeftSibling();
                  } else {
                    leftSibling = -1;
                  }
                } else {
                  leftSibling = -1;
                }
              } else {
                leftSibling = bucket.getLeftSibling();
              }
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }

          while (rightSibling >= 0) {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, rightSibling, false, true);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.getKey(0).equals(key)) {
                  removed += bucket.remove(0);

                  if (size <= 1) {
                    rightSibling = bucket.getRightSibling();
                  } else {
                    rightSibling = -1;
                  }
                } else {
                  rightSibling = -1;
                }
              } else {
                rightSibling = bucket.getRightSibling();
              }
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }

          if (removed > 0) {
            updateSize(-removed, atomicOperation);
          }

          return removed > 0;
        } else {
          final int removed;
          final OCacheEntry nullCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
          try {
            final ONullBucket nullBucket = new ONullBucket(nullCacheEntry, multiContainer);
            removed = nullBucket.remove();
          } finally {
            releasePageFromWrite(atomicOperation, nullCacheEntry);
          }

          updateSize(-removed, atomicOperation);

          return removed > 0;
        }
      } finally {
        releaseExclusiveLock();
      }
    } catch (final RuntimeException | IOException e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }
  }

  public boolean remove(K key, final ORID value) throws IOException {
    boolean rollback = false;
    final OAtomicOperation atomicOperation = startAtomicOperation(true);

    try {
      boolean removed;
      acquireExclusiveLock();
      try {
        if (key != null) {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
          if (bucketSearchResult.itemIndex < 0) {
            return false;
          }

          final long pageIndex = bucketSearchResult.pageIndex;
          final int itemIndex = bucketSearchResult.itemIndex;

          long leftSibling = -1;
          long rightSibling = -1;

          OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
          try {
            final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);

            removed = bucket.remove(itemIndex, value);

            if (!removed) {
              if (itemIndex == 0) {
                leftSibling = bucket.getLeftSibling();
              }

              if (itemIndex == bucket.size() - 1) {
                rightSibling = bucket.getRightSibling();
              }
            }
          } finally {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          while (!removed && leftSibling >= 0) {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, leftSibling, false, true);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.remove(size - 1, value)) {
                  removed = true;
                }

                if (!removed) {
                  if (size <= 1) {
                    leftSibling = bucket.getLeftSibling();
                  } else {
                    leftSibling = -1;
                  }
                }
              } else {
                leftSibling = bucket.getLeftSibling();
              }
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }

          while (!removed && rightSibling >= 0) {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, rightSibling, false, true);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              final int size = bucket.size();

              if (size > 0) {
                if (bucket.remove(0, value)) {
                  removed = true;
                }

                if (!removed) {
                  if (size <= 1) {
                    rightSibling = bucket.getRightSibling();
                  } else {
                    rightSibling = -1;
                  }
                }
              } else {
                rightSibling = bucket.getRightSibling();
              }
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }

          if (removed) {
            updateSize(-1, atomicOperation);
          }
        } else {
          final OCacheEntry nullBucketCacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, false, true);
          try {
            final ONullBucket nullBucket = new ONullBucket(nullBucketCacheEntry, multiContainer);
            removed = nullBucket.removeValue(value);
          } finally {
            releasePageFromWrite(atomicOperation, nullBucketCacheEntry);
          }

          if (removed) {
            updateSize(-1, atomicOperation);
          }
        }
      } finally {
        releaseExclusiveLock();
      }

      return removed;
    } catch (final RuntimeException | IOException e) {
      rollback = true;
      throw e;
    } finally {
      endAtomicOperation(rollback);
    }

  }

  public OCellBTreeCursor<K, ORID> iterateEntriesMinor(final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (!ascSortOrder) {
          return iterateEntriesMinorDesc(key, inclusive);
        }

        return iterateEntriesMinorAsc(key, inclusive);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public OCellBTreeCursor<K, ORID> iterateEntriesMajor(final K key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return iterateEntriesMajorAsc(key, inclusive);
        }

        return iterateEntriesMajorDesc(key, inclusive);
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
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.pageIndex, false);
        try {
          final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
          return bucket.getKey(searchResult.itemIndex);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeMultiValueException("Error during finding first key in sbtree [" + getName() + "]", this),
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
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final BucketSearchResult searchResult = lastItem(atomicOperation);
        if (searchResult == null) {
          return null;
        }

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, searchResult.pageIndex, false);
        try {
          final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
          return bucket.getKey(searchResult.itemIndex);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeMultiValueException("Error during finding last key in sbtree [" + getName() + "]", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public OCellBTreeKeyCursor<K> keyCursor() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final BucketSearchResult searchResult = firstItem(atomicOperation);
        if (searchResult == null) {
          return prefetchSize -> null;
        }

        return new OCellBTreeFullKeyCursor(searchResult.pageIndex);
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException
          .wrapException(new OCellBTreeMultiValueException("Error during finding first key in sbtree [" + getName() + "]", this),
              e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public OCellBTreeCursor<K, ORID> iterateEntriesBetween(final K keyFrom, final boolean fromInclusive, final K keyTo,
      final boolean toInclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive);
        } else {
          return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive);
        }
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

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMinorDesc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    return new OCellBTreeCursorBackward(null, key, false, inclusive);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMinorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    return new OCellBTreeCursorForward(null, key, false, inclusive);
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

  private OCellBTreeCursor<K, ORID> iterateEntriesMajorAsc(K key, final boolean inclusive) {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    return new OCellBTreeCursorForward(key, null, inclusive, false);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesMajorDesc(K key, final boolean inclusive) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);
      key = enhanceCompositeKeyMajorDesc(key, inclusive);

      return new OCellBTreeCursorBackward(key, null, inclusive, false);

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

  private BucketSearchResult firstItem(final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);
    int itemIndex = 0;
    try {
      Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
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

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(0, bucketIndex);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private BucketSearchResult lastItem(final OAtomicOperation atomicOperation) throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

    Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              bucketIndex = bucket.getRight(itemIndex);
            } else {
              bucketIndex = bucket.getLeft(0);
            }

            itemIndex = Bucket.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return null;
            }
          } else {
            return new BucketSearchResult(bucket.size() - 1, bucketIndex);
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
        if (itemIndex == Bucket.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesBetweenAscOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    return new OCellBTreeCursorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OCellBTreeCursor<K, ORID> iterateEntriesBetweenDescOrder(K keyFrom, final boolean fromInclusive, K keyTo,
      final boolean toInclusive) {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    return new OCellBTreeCursorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
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

  private UpdateBucketSearchResult splitBucket(final Bucket<K> bucketToSplit, final OCacheEntry entryToSplit, final List<Long> path,
      final List<Integer> insertionIndexes, final int keyIndex, final K keyToInsert, final OAtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit = bucketSize >>> 1;
    final byte[] serializedSeparationKey = bucketToSplit.getRawKey(indexToSplit);

    final List<Bucket.Entry> rightEntries = new ArrayList<>(indexToSplit);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    if (splitLeaf) {
      for (int i = startRightIndex; i < bucketSize; i++) {
        rightEntries.add(bucketToSplit.getLeafEntry(i));
      }
    } else {
      for (int i = startRightIndex; i < bucketSize; i++) {
        rightEntries.add(bucketToSplit.getNonLeafEntry(i));
      }
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(path, insertionIndexes, keyIndex, keyToInsert, entryToSplit.getPageIndex(), bucketToSplit,
          splitLeaf, indexToSplit, serializedSeparationKey, rightEntries, atomicOperation);
    } else {
      return splitRootBucket(keyIndex, keyToInsert, entryToSplit, bucketToSplit, splitLeaf, indexToSplit, serializedSeparationKey,
          rightEntries, atomicOperation);
    }
  }

  private UpdateBucketSearchResult splitNonRootBucket(final List<Long> path, final List<Integer> insertionIndexes,
      final int keyIndex, final K keyToInsert, final long pageIndex, final Bucket<K> bucketToSplit, final boolean splitLeaf,
      final int indexToSplit, final byte[] serializedSeparationKey, final List<Bucket.Entry> rightEntries,
      final OAtomicOperation atomicOperation) throws IOException {

    final OCacheEntry rightBucketEntry;
    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
      final int pageSize = entryPoint.getPagesSize();

      if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false, false);
        entryPoint.setPagesSize(pageSize + 1);
      } else {
        assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

        rightBucketEntry = addPage(atomicOperation, fileId);
        entryPoint.setPagesSize((int) rightBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }

    K separationKey = null;

    try {
      final Bucket<K> newRightBucket = new Bucket<>(rightBucketEntry, splitLeaf, keySerializer, multiContainer);
      newRightBucket.addAll(rightEntries);

      assert bucketToSplit.size() > 1;
      bucketToSplit.shrink(indexToSplit);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, false, true);
          final Bucket<K> rightSiblingBucket = new Bucket<>(rightSiblingBucketEntry, keySerializer, multiContainer);
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          } finally {
            releasePageFromWrite(atomicOperation, rightSiblingBucketEntry);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
      try {
        Bucket<K> parentBucket = new Bucket<>(parentCacheEntry, keySerializer, multiContainer);
        int insertionIndex = insertionIndexes.get(insertionIndexes.size() - 2);

        while (!parentBucket
            .addNonLeafEntry(insertionIndex, serializedSeparationKey, (int) pageIndex, (int) rightBucketEntry.getPageIndex(),
                true)) {
          if (separationKey == null) {
            separationKey = keySerializer.deserializeNativeObject(serializedSeparationKey, 0);
          }

          final UpdateBucketSearchResult bucketSearchResult = splitBucket(parentBucket, parentCacheEntry,
              path.subList(0, path.size() - 1), insertionIndexes.subList(0, insertionIndexes.size() - 1), insertionIndex,
              separationKey, atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          insertionIndex = bucketSearchResult.itemIndex;

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            releasePageFromWrite(atomicOperation, parentCacheEntry);
            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
          }

          parentBucket = new Bucket<>(parentCacheEntry, keySerializer, multiContainer);
        }

      } finally {
        releasePageFromWrite(atomicOperation, parentCacheEntry);
      }

    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
    final ArrayList<Integer> resultInsertionIndexes = new ArrayList<>(insertionIndexes.subList(0, insertionIndexes.size() - 1));

    if (keyIndex < indexToSplit) {
      return addToTheLeftNonRootBucket(keyIndex, pageIndex, resultPath, resultInsertionIndexes);
    } else if (keyIndex > indexToSplit) {
      return addToTheRightNonRootBucket(keyIndex, splitLeaf, indexToSplit, rightBucketEntry.getPageIndex(), resultPath,
          resultInsertionIndexes);
    } else if (splitLeaf && keyToInsert.equals(separationKey != null ? separationKey :
        keySerializer.deserializeNativeObject(serializedSeparationKey, 0))) {
      return addToTheRightNonRootBucket(keyIndex, true, indexToSplit, rightBucketEntry.getPageIndex(), resultPath,
          resultInsertionIndexes);
    } else {
      return addToTheLeftNonRootBucket(keyIndex, pageIndex, resultPath, resultInsertionIndexes);
    }
  }

  private static UpdateBucketSearchResult addToTheRightNonRootBucket(final int keyIndex, final boolean splitLeaf,
      final int indexToSplit, final long rightPageIndex, final ArrayList<Long> resultPath,
      final ArrayList<Integer> resultItemPointers) {
    final int parentIndex = resultItemPointers.size() - 1;
    resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
    resultPath.add(rightPageIndex);

    if (splitLeaf) {
      resultItemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
    }

    final int insertionIndex = keyIndex - indexToSplit - 1;

    resultItemPointers.add(insertionIndex);
    return new UpdateBucketSearchResult(resultItemPointers, resultPath, insertionIndex);
  }

  private static UpdateBucketSearchResult addToTheLeftNonRootBucket(final int keyIndex, final long pageIndex,
      final ArrayList<Long> resultPath, final ArrayList<Integer> resultItemPointers) {
    resultPath.add(pageIndex);
    resultItemPointers.add(keyIndex);

    return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
  }

  private UpdateBucketSearchResult splitRootBucket(final int keyIndex, final K keyToInsert, final OCacheEntry bucketEntry,
      Bucket<K> bucketToSplit, final boolean splitLeaf, final int indexToSplit, final byte[] serializedSeparationKey,
      final List<Bucket.Entry> rightEntries, final OAtomicOperation atomicOperation) throws IOException {
    final List<Bucket.Entry> leftEntries = new ArrayList<>(indexToSplit);

    if (splitLeaf) {
      if (bucketToSplit.size() > 1) {
        for (int i = 0; i < indexToSplit; i++) {
          leftEntries.add(bucketToSplit.getLeafEntry(i));
        }
      } else {
        final Bucket.LeafEntry currentEntry = bucketToSplit.getLeafEntry(0);
        final List<ORID> values = currentEntry.values;

        final long mId = incrementMId(atomicOperation);
        final Bucket.LeafEntry newEntry = new Bucket.LeafEntry(currentEntry.key, mId,
            currentEntry.values.subList(values.size() / 2, values.size()));

        leftEntries.add(newEntry);
      }
    } else {
      for (int i = 0; i < indexToSplit; i++) {
        leftEntries.add(bucketToSplit.getNonLeafEntry(i));
      }
    }

    final OCacheEntry leftBucketEntry;
    final OCacheEntry rightBucketEntry;

    final OCacheEntry entryPointCacheEntry = loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
      int pageSize = entryPoint.getPagesSize();

      final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

      if (pageSize < filledUpTo - 1) {
        leftBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false, false);
        pageSize++;
      } else {
        assert pageSize == filledUpTo - 1;

        leftBucketEntry = addPage(atomicOperation, fileId);
        pageSize = (int) leftBucketEntry.getPageIndex();
      }

      if (pageSize < filledUpTo) {
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false, false);
        pageSize++;
      } else {
        assert pageSize == filledUpTo;

        rightBucketEntry = addPage(atomicOperation, fileId);
        pageSize = (int) rightBucketEntry.getPageIndex();
      }

      entryPoint.setPagesSize(pageSize);
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }

    try {
      final Bucket<K> newLeftBucket = new Bucket<>(leftBucketEntry, splitLeaf, keySerializer, multiContainer);
      newLeftBucket.addAll(leftEntries);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      releasePageFromWrite(atomicOperation, leftBucketEntry);
    }

    try {
      final Bucket<K> newRightBucket = new Bucket<>(rightBucketEntry, splitLeaf, keySerializer, multiContainer);
      newRightBucket.addAll(rightEntries);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    bucketToSplit = new Bucket<>(bucketEntry, false, keySerializer, multiContainer);

    bucketToSplit
        .addNonLeafEntry(0, serializedSeparationKey, (int) leftBucketEntry.getPageIndex(), (int) rightBucketEntry.getPageIndex(),
            true);

    final ArrayList<Long> resultPath = new ArrayList<>(8);
    resultPath.add(ROOT_INDEX);

    final ArrayList<Integer> itemPointers = new ArrayList<>(8);

    if (keyIndex < indexToSplit) {
      return addToTheLeftRootBucket(keyIndex, leftBucketEntry, resultPath, itemPointers);
    } else if (keyIndex > indexToSplit) {
      return addToTheRightRootBucket(keyIndex, splitLeaf, indexToSplit, rightBucketEntry, resultPath, itemPointers);
    } else if (splitLeaf && keyToInsert.equals(keySerializer.deserializeNativeObject(serializedSeparationKey, 0))) {
      return addToTheRightRootBucket(keyIndex, true, indexToSplit, rightBucketEntry, resultPath, itemPointers);
    } else {
      return addToTheLeftRootBucket(keyIndex, leftBucketEntry, resultPath, itemPointers);
    }
  }

  private static UpdateBucketSearchResult addToTheRightRootBucket(final int keyIndex, final boolean splitLeaf,
      final int indexToSplit, final OCacheEntry rightBucketEntry, final ArrayList<Long> resultPath,
      final ArrayList<Integer> itemPointers) {
    resultPath.add(rightBucketEntry.getPageIndex());
    itemPointers.add(0);

    if (splitLeaf) {
      itemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
    }

    final int itemPointer = keyIndex - indexToSplit - 1;
    itemPointers.add(itemPointer);

    return new UpdateBucketSearchResult(itemPointers, resultPath, itemPointer);
  }

  private static UpdateBucketSearchResult addToTheLeftRootBucket(final int keyIndex, final OCacheEntry leftBucketEntry,
      final ArrayList<Long> resultPath, final ArrayList<Integer> itemPointers) {
    itemPointers.add(-1);
    itemPointers.add(keyIndex);

    resultPath.add(leftBucketEntry.getPageIndex());
    return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
  }

  private BucketSearchResult findBucket(final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new OCellBTreeMultiValueException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket<K> keyBucket = new Bucket<>(bucketEntry, keySerializer, multiContainer);
        final int index = keyBucket.find(key);

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
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private UpdateBucketSearchResult findBucketForUpdate(final K key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    final ArrayList<Long> path = new ArrayList<>(8);
    final ArrayList<Integer> insertionIndexes = new ArrayList<>(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new OCellBTreeMultiValueException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);
      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket<K> keyBucket = new Bucket<>(bucketEntry, keySerializer, multiContainer);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf()) {
          insertionIndexes.add(index);
          return new UpdateBucketSearchResult(insertionIndexes, path, index);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
          insertionIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }

          insertionIndexes.add(insertionIndex);
        }
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private K enhanceCompositeKey(final K key, final PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey)) {
      return key;
    }

    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (!(keySize == 1 || compositeKey.getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
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
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether lowest
   * or highest partially matched key should be used.
   */
  private enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE,
    /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  private static final class BucketSearchResult {
    private final int  itemIndex;
    private final long pageIndex;

    private BucketSearchResult(final int itemIndex, final long pageIndex) {
      this.itemIndex = itemIndex;
      this.pageIndex = pageIndex;
    }
  }

  private static final class UpdateBucketSearchResult {
    private final List<Integer>   insertionIndexes;
    private final ArrayList<Long> path;
    private final int             itemIndex;

    private UpdateBucketSearchResult(final List<Integer> insertionIndexes, final ArrayList<Long> path, final int itemIndex) {
      this.insertionIndexes = insertionIndexes;
      this.path = path;
      this.itemIndex = itemIndex;
    }

    final long getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class PagePathItemUnit {
    private final long pageIndex;
    private final int  itemIndex;

    private PagePathItemUnit(final long pageIndex, final int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  public final class OCellBTreeFullKeyCursor implements OCellBTreeKeyCursor<K> {
    private long pageIndex;
    private int  itemIndex;

    private List<K>     keysCache    = new ArrayList<>();
    private Iterator<K> keysIterator = new OEmptyIterator<>();

    OCellBTreeFullKeyCursor(final long startPageIndex) {
      pageIndex = startPageIndex;
      itemIndex = 0;
    }

    @Override
    public K next(int prefetchSize) {
      if (keysIterator == null) {
        return null;
      }

      if (keysIterator.hasNext()) {
        return keysIterator.next();
      }

      keysCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      if (prefetchSize == 0) {
        prefetchSize = 1;
      }

      atomicOperationsManager.acquireReadLock(OCellBTreeMultiValueV3.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          while (keysCache.size() < prefetchSize) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry entryPointCacheEntry = loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
            try {
              final OEntryPoint<K> entryPoint = new OEntryPoint<>(entryPointCacheEntry);
              if (pageIndex >= entryPoint.getPagesSize() + 1) {
                pageIndex = -1;
                break;
              }
            } finally {
              releasePageFromRead(atomicOperation, entryPointCacheEntry);
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);

              final int bucketSize = bucket.size();
              if (itemIndex >= bucketSize) {
                pageIndex = bucket.getRightSibling();
                itemIndex = 0;
                continue;
              }

              while (itemIndex < bucketSize && keysCache.size() < prefetchSize) {
                keysCache.add(keySerializer.deserializeNativeObject(bucket.getRawKey(itemIndex), 0));
                itemIndex++;
              }
            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeMultiValueException("Error during element iteration", OCellBTreeMultiValueV3.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OCellBTreeMultiValueV3.this);
      }

      if (keysCache.isEmpty()) {
        keysCache = null;
        return null;
      }

      keysIterator = keysCache.iterator();
      return keysIterator.next();
    }
  }

  private final class OCellBTreeCursorForward implements OCellBTreeCursor<K, ORID> {
    private       K       fromKey;
    private final K       toKey;
    private       boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private final List<Map.Entry<K, ORID>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, ORID>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OCellBTreeCursorForward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (fromKey == null) {
        this.fromKeyInclusive = true;
      }
    }

    public Map.Entry<K, ORID> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, ORID> entry = dataCacheIterator.next();

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

      atomicOperationsManager.acquireReadLock(OCellBTreeMultiValueV3.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (fromKey != null) {
            bucketSearchResult = findBucket(fromKey, atomicOperation);
          } else {
            bucketSearchResult = firstItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.pageIndex;
          int itemIndex;

          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = fromKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 1;
          }

          K lastKey = null;

          boolean firstTry = true;
          mainCycle:
          while (true) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              if (firstTry && fromKey != null && fromKeyInclusive && bucketSearchResult.itemIndex == 0) {
                int leftSibling = (int) bucket.getLeftSibling();
                while (leftSibling > 0) {
                  final OCacheEntry siblingCacheEntry = loadPageForRead(atomicOperation, fileId, leftSibling, false);
                  final Bucket<K> siblingBucket = new Bucket<>(siblingCacheEntry, keySerializer, multiContainer);

                  final int bucketSize = siblingBucket.size();
                  if (bucketSize == 0) {
                    leftSibling = (int) siblingBucket.getLeftSibling();
                  } else if (bucketSize == 1) {
                    final K key = siblingBucket.getKey(0);

                    if (key.equals(fromKey)) {
                      lastKey = key;

                      for (final ORID rid : siblingBucket.getValues(0)) {
                        dataCache.add(new Map.Entry<K, ORID>() {
                          @Override
                          public K getKey() {
                            return key;
                          }

                          @Override
                          public ORID getValue() {
                            return rid;
                          }

                          @Override
                          public ORID setValue(final ORID value) {
                            throw new UnsupportedOperationException("setValue");
                          }
                        });
                      }

                      leftSibling = (int) siblingBucket.getLeftSibling();
                    } else {
                      leftSibling = -1;
                    }
                  } else {
                    final K key = siblingBucket.getKey(bucketSize - 1);
                    if (key.equals(fromKey)) {
                      lastKey = key;

                      for (final ORID rid : siblingBucket.getValues(0)) {
                        dataCache.add(new Map.Entry<K, ORID>() {
                          @Override
                          public K getKey() {
                            return key;
                          }

                          @Override
                          public ORID getValue() {
                            return rid;
                          }

                          @Override
                          public ORID setValue(final ORID value) {
                            throw new UnsupportedOperationException("setValue");
                          }
                        });
                      }

                      leftSibling = -1;
                    } else {
                      leftSibling = -1;
                    }
                  }

                  releasePageFromRead(atomicOperation, siblingCacheEntry);
                }
              }

              firstTry = false;

              while (true) {
                if (itemIndex >= bucket.size()) {
                  pageIndex = bucket.getRightSibling();
                  itemIndex = 0;
                  continue mainCycle;
                }

                final Bucket.LeafEntry leafEntry = bucket.getLeafEntry(itemIndex);
                itemIndex++;

                final K key = keySerializer.deserializeNativeObject(leafEntry.key, 0);
                if (dataCache.size() >= prefetchSize && (lastKey == null || !lastKey.equals(key))) {
                  break mainCycle;
                }

                if (fromKey != null && (fromKeyInclusive ?
                    comparator.compare(key, fromKey) < 0 :
                    comparator.compare(key, fromKey) <= 0)) {
                  continue;
                }

                if (toKey != null && (toKeyInclusive ? comparator.compare(key, toKey) > 0 : comparator.compare(key, toKey) >= 0)) {
                  break mainCycle;
                }

                lastKey = key;
                for (final ORID rid : leafEntry.values) {
                  dataCache.add(new Map.Entry<K, ORID>() {
                    @Override
                    public K getKey() {
                      return key;
                    }

                    @Override
                    public ORID getValue() {
                      return rid;
                    }

                    @Override
                    public ORID setValue(final ORID value) {
                      throw new UnsupportedOperationException("setValue");
                    }
                  });
                }
              }

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeMultiValueException("Error during element iteration", OCellBTreeMultiValueV3.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OCellBTreeMultiValueV3.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, ORID> entry = dataCacheIterator.next();

      fromKey = entry.getKey();
      fromKeyInclusive = false;

      return entry;
    }
  }

  private final class OCellBTreeCursorBackward implements OCellBTreeCursor<K, ORID> {
    private final K       fromKey;
    private       K       toKey;
    private final boolean fromKeyInclusive;
    private       boolean toKeyInclusive;

    private final List<Map.Entry<K, ORID>>     dataCache         = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private       Iterator<Map.Entry<K, ORID>> dataCacheIterator = OEmptyMapEntryIterator.INSTANCE;

    private OCellBTreeCursorBackward(final K fromKey, final K toKey, final boolean fromKeyInclusive, final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;

      if (toKey == null) {
        this.toKeyInclusive = true;
      }

    }

    public Map.Entry<K, ORID> next(int prefetchSize) {
      if (dataCacheIterator == null) {
        return null;
      }

      if (dataCacheIterator.hasNext()) {
        final Map.Entry<K, ORID> entry = dataCacheIterator.next();
        toKey = entry.getKey();

        toKeyInclusive = false;
        return entry;
      }

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger()) {
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();
      }

      atomicOperationsManager.acquireReadLock(OCellBTreeMultiValueV3.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

          final BucketSearchResult bucketSearchResult;

          if (toKey != null) {
            bucketSearchResult = findBucket(toKey, atomicOperation);
          } else {
            bucketSearchResult = lastItem(atomicOperation);
          }

          if (bucketSearchResult == null) {
            dataCacheIterator = null;
            return null;
          }

          long pageIndex = bucketSearchResult.pageIndex;

          int itemIndex;
          if (bucketSearchResult.itemIndex >= 0) {
            itemIndex = toKeyInclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
          } else {
            itemIndex = -bucketSearchResult.itemIndex - 2;
          }

          boolean firstTry = true;
          K lastKey = null;
          mainCycle:
          while (true) {
            if (pageIndex == -1) {
              break;
            }

            final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
              final Bucket<K> bucket = new Bucket<>(cacheEntry, keySerializer, multiContainer);
              if (firstTry && toKey != null && toKeyInclusive && bucketSearchResult.itemIndex == 0) {
                int rightSibling = (int) bucket.getRightSibling();
                while (rightSibling > 0) {
                  final OCacheEntry siblingCacheEntry = loadPageForRead(atomicOperation, fileId, rightSibling, false);
                  final Bucket<K> siblingBucket = new Bucket<>(siblingCacheEntry, keySerializer, multiContainer);

                  final int bucketSize = siblingBucket.size();
                  if (bucketSize == 0) {
                    rightSibling = (int) siblingBucket.getRightSibling();
                  } else if (bucketSize == 1) {
                    final K key = siblingBucket.getKey(0);

                    if (key.equals(fromKey)) {
                      lastKey = key;

                      for (final ORID rid : siblingBucket.getValues(0)) {
                        dataCache.add(new Map.Entry<K, ORID>() {
                          @Override
                          public K getKey() {
                            return key;
                          }

                          @Override
                          public ORID getValue() {
                            return rid;
                          }

                          @Override
                          public ORID setValue(final ORID value) {
                            throw new UnsupportedOperationException("setValue");
                          }
                        });
                      }

                      rightSibling = (int) siblingBucket.getRightSibling();
                    } else {
                      rightSibling = -1;
                    }
                  } else {
                    final K key = siblingBucket.getKey(0);
                    if (key.equals(fromKey)) {
                      lastKey = key;

                      for (final ORID rid : siblingBucket.getValues(0)) {
                        dataCache.add(new Map.Entry<K, ORID>() {
                          @Override
                          public K getKey() {
                            return key;
                          }

                          @Override
                          public ORID getValue() {
                            return rid;
                          }

                          @Override
                          public ORID setValue(final ORID value) {
                            throw new UnsupportedOperationException("setValue");
                          }
                        });
                      }

                      rightSibling = -1;
                    } else {
                      rightSibling = -1;
                    }

                    releasePageFromRead(atomicOperation, siblingCacheEntry);
                  }
                }
              }

              firstTry = false;

              while (true) {
                if (itemIndex >= bucket.size()) {
                  itemIndex = bucket.size() - 1;
                }

                if (itemIndex < 0) {
                  pageIndex = bucket.getLeftSibling();
                  itemIndex = Integer.MAX_VALUE;
                  continue mainCycle;
                }

                final Bucket.LeafEntry leafEntry = bucket.getLeafEntry(itemIndex);
                itemIndex--;

                final K key = keySerializer.deserializeNativeObject(leafEntry.key, 0);
                if (dataCache.size() >= prefetchSize && (lastKey == null || !lastKey.equals(key))) {
                  break mainCycle;
                }

                if (toKey != null && (toKeyInclusive ? comparator.compare(key, toKey) > 0 : comparator.compare(key, toKey) >= 0)) {
                  continue;
                }

                if (fromKey != null && (fromKeyInclusive ?
                    comparator.compare(key, fromKey) < 0 :
                    comparator.compare(key, fromKey) <= 0)) {
                  break mainCycle;
                }

                lastKey = key;

                for (final ORID rid : leafEntry.values) {
                  dataCache.add(new Map.Entry<K, ORID>() {
                    @Override
                    public K getKey() {
                      return key;
                    }

                    @Override
                    public ORID getValue() {
                      return rid;
                    }

                    @Override
                    public ORID setValue(final ORID value) {
                      throw new UnsupportedOperationException("setValue");
                    }
                  });
                }
              }

            } finally {
              releasePageFromRead(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException
            .wrapException(new OCellBTreeMultiValueException("Error during element iteration", OCellBTreeMultiValueV3.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(OCellBTreeMultiValueV3.this);
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      final Map.Entry<K, ORID> entry = dataCacheIterator.next();

      toKey = entry.getKey();
      toKeyInclusive = false;

      return entry;
    }
  }
}
