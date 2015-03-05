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
package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Implementation of hash index which is based on <a href="http://en.wikipedia.org/wiki/Extendible_hashing">extendible hashing
 * algorithm</a>. The directory for extindible hashing is implemented in
 * {@link com.orientechnologies.orient.core.index.hashindex.local.OHashTableDirectory} class. Directory is not implemented according
 * to classic algorithm because of its big memory consumption in case of non-uniform data distribution instead it is implemented
 * according too "Multilevel Extendible Hashing Sven Helmer, Thomas Neumann, Guido Moerkotte April 17, 2002". Which has much less
 * memory consumption in case of nonuniform data distribution.
 * 
 * Index itself uses so called "muiltilevel  schema" when first level contains 256 buckets, when bucket is split it is put at the
 * end of other file which represents second level. So if data which are put has distribution close to uniform (this index was
 * designed to be use as rid index for DHT storage) buckets split will be preformed in append only manner to speed up index write
 * speed.
 * 
 * So hash index bucket itself has following structure:
 * <ol>
 * <li>Bucket depth - 1 byte.</li>
 * <li>Bucket's size - amount of entities (key, value) in one bucket, 4 bytes</li>
 * <li>Page indexes of parents of this bucket, page indexes of buckets split of which created current bucket - 64*8 bytes.</li>
 * <li>Offsets of entities stored in this bucket relatively to it's beginning. It is array of int values of undefined size.</li>
 * <li>Entities itself</li>
 * </ol>
 * 
 * So if 1-st and 2-nd fields are clear. We should discuss the last ones.
 * 
 * 
 * Entities in bucket are sorted by key's hash code so each entity has following storage format in bucket: key's hash code (8
 * bytes), key, value. Because entities are stored in sorted order it means that every time when we insert new entity old ones
 * should be moved.
 * 
 * There are 2 reasons why it is bad:
 * <ol>
 * <li>It will generate write ahead log of enormous size.</li>
 * <li>The more amount of memory is affected in operation the less speed we will have. In worst case 60 kb of memory should be
 * moved.</li>
 * </ol>
 * 
 * To avoid disadvantages listed above entries ara appended to the end of bucket, but their offsets are stored at the beginning of
 * bucket. Offsets are stored in sorted order (ordered by hash code of entity's key) so we need to move only small amount of memory
 * to store entities in sorted order.
 * 
 * About indexes of parents of current bucket. When item is removed from bucket we check space which is needed to store all entities
 * of this bucket, it's buddy bucket (bucket which was also created from parent bucket during split) and if space of single bucket
 * is enough to save all entities from both buckets we remove these buckets and put all content in parent bucket. That is why we
 * need indexes of parents of current bucket.
 * 
 * Also hash index has special file of one page long which contains information about state of each level of buckets in index. This
 * information is stored as array index of which equals to file level. All array item has following structure:
 * <ol>
 * <li>Is level removed (in case all buckets are empty or level was not created yet) - 1 byte</li>
 * <li>File's level id - 8 bytes</li>
 * <li>Amount of buckets in given level - 8 bytes.</li>
 * <li>Index of page of first removed bucket (not splitted but removed) - 8 bytes</li>
 * </ol>
 * 
 * 
 * @author Andrey Lomakin
 * @since 12.03.13
 */
public class OLocalHashTable<K, V> extends ODurableComponent {
  private static final double            MERGE_THRESHOLD     = 0.2;

  private static final long              HASH_CODE_MIN_VALUE = 0;
  private static final long              HASH_CODE_MAX_VALUE = 0xFFFFFFFFFFFFFFFFL;

  private final String                   metadataConfigurationFileExtension;
  private final String                   treeStateFileExtension;
  private final String                   bucketFileExtension;

  public static final int                HASH_CODE_SIZE      = 64;
  public static final int                MAX_LEVEL_DEPTH     = 8;
  public static final int                MAX_LEVEL_SIZE      = 1 << MAX_LEVEL_DEPTH;

  public static final int                LEVEL_MASK          = Integer.MAX_VALUE >>> (31 - MAX_LEVEL_DEPTH);

  private OAbstractPaginatedStorage      storage;

  private String                         name;

  private ODiskCache                     diskCache;
  private final OHashFunction<K>         keyHashFunction;

  private OBinarySerializer<K>           keySerializer;
  private OBinarySerializer<V>           valueSerializer;
  private OType[]                        keyTypes;

  private final KeyHashCodeComparator<K> comparator;

  private boolean                        nullKeyIsSupported;
  private long                           nullBucketFileId    = -1;
  private final String                   nullBucketFileExtension;

  private long                           fileStateId;

  private OCacheEntry                    hashStateEntry;

  private OHashTableDirectory            directory;

  private final boolean                  durableInNonTxMode;
  private final ODurablePage.TrackMode   trackMode;

  public OLocalHashTable(String metadataConfigurationFileExtension, String treeStateFileExtension, String bucketFileExtension,
      String nullBucketFileExtension, OHashFunction<K> keyHashFunction, boolean durableInNonTxMode, ODurablePage.TrackMode trackMode) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.metadataConfigurationFileExtension = metadataConfigurationFileExtension;
    this.treeStateFileExtension = treeStateFileExtension;
    this.bucketFileExtension = bucketFileExtension;
    this.keyHashFunction = keyHashFunction;
    this.nullBucketFileExtension = nullBucketFileExtension;
    this.durableInNonTxMode = durableInNonTxMode;

    this.comparator = new KeyHashCodeComparator<K>(this.keyHashFunction);

    if (trackMode == null) {
        this.trackMode = ODurablePage.TrackMode.valueOf(OGlobalConfiguration.INDEX_TX_MODE.getValueAsString().toUpperCase());
    } else {
        this.trackMode = trackMode;
    }
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      OAbstractPaginatedStorage storageLocal, boolean nullKeyIsSupported) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.keyTypes = keyTypes;
      this.nullKeyIsSupported = nullKeyIsSupported;

      this.diskCache = storage.getDiskCache();
      if (this.diskCache == null) {
          throw new IllegalStateException("Disk cache was not initialized on storage level");
      }

      this.name = name;

      init(storage);
      this.directory = new OHashTableDirectory(treeStateFileExtension, name, durableInNonTxMode, storage);

      startAtomicOperation();
      try {
        fileStateId = diskCache.openFile(name + metadataConfigurationFileExtension);
        logFileCreation(name + metadataConfigurationFileExtension, fileStateId);

        directory.create();

        hashStateEntry = diskCache.allocateNewPage(fileStateId);
        diskCache.pinPage(hashStateEntry);

        hashStateEntry.acquireExclusiveLock();
        try {
          OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), true);

          createFileMetadata(0, page);
          hashStateEntry.markDirty();

          logPageChanges(page, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), true);
        } finally {
          hashStateEntry.releaseExclusiveLock();
          diskCache.release(hashStateEntry);
        }

        setKeySerializer(keySerializer);
        setValueSerializer(valueSerializer);

        initHashTreeState();

        if (nullKeyIsSupported) {
          nullBucketFileId = diskCache.openFile(name + nullBucketFileExtension);
          logFileCreation(name + nullBucketFileExtension, nullBucketFileId);
        }

        endAtomicOperation(false);
      } catch (IOException e) {
        endAtomicOperation(true);
        throw e;
      } catch (Throwable e) {
        endAtomicOperation(true);
        throw new OStorageException(null, e);
      }

    } catch (IOException e) {
      throw new OIndexException("Error during local hash table creation.", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    final OStorageTransaction transaction = storage.getStorageTransaction();

    if (transaction == null && !durableInNonTxMode) {
        return ODurablePage.TrackMode.NONE;
    }

    final ODurablePage.TrackMode trackMode = super.getTrackMode();
    if (!trackMode.equals(ODurablePage.TrackMode.NONE)) {
        return this.trackMode;
    }

    return trackMode;
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
  protected void logFileCreation(String fileName, long fileId) throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode) {
        return;
    }

    super.logFileCreation(fileName, fileId);
  }

  @Override
  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    final OStorageTransaction transaction = storage.getStorageTransaction();

    if (transaction == null && !durableInNonTxMode) {
        return;
    }

    super.logPageChanges(localPage, fileId, pageIndex, isNewPage);
  }

  public OBinarySerializer<K> getKeySerializer() {
    acquireSharedLock();
    try {
      return keySerializer;
    } finally {
      releaseSharedLock();
    }
  }

  public void setKeySerializer(OBinarySerializer<K> keySerializer) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();

      this.keySerializer = keySerializer;
      diskCache.loadPinnedPage(hashStateEntry);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);

        metadataPage.setKeySerializerId(keySerializer.getId());
        hashStateEntry.markDirty();

        logPageChanges(metadataPage, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        diskCache.release(hashStateEntry);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();

      throw new OIndexException("Can not set serializer for index keys", e);
    } catch (Throwable e) {
      rollback();
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void rollback() {
    try {
      endAtomicOperation(true);
    } catch (IOException ioe) {
      throw new OIndexException("Error during operation roolback", ioe);
    }
  }

  public OBinarySerializer<V> getValueSerializer() {
    acquireSharedLock();
    try {
      return valueSerializer;
    } finally {
      releaseSharedLock();
    }
  }

  public void setValueSerializer(OBinarySerializer<V> valueSerializer) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      this.valueSerializer = valueSerializer;
      diskCache.loadPinnedPage(hashStateEntry);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);

        metadataPage.setValueSerializerId(valueSerializer.getId());
        hashStateEntry.markDirty();

        logPageChanges(metadataPage, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        diskCache.release(hashStateEntry);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();
      throw new OIndexException("Can not set serializer for index values", e);
    } catch (Throwable e) {
      rollback();
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void createFileMetadata(int fileLevel, OHashIndexFileLevelMetadataPage page) throws IOException {
    final String fileName = name + fileLevel + bucketFileExtension;
    final long fileId = diskCache.openFile(fileName);

    logFileCreation(fileName, fileId);

    page.setFileMetadata(fileLevel, fileId, 0, -1);
  }

  public V get(K key) {

    acquireSharedLock();
    try {
      checkNullSupport(key);
      if (key == null) {
        if (diskCache.getFilledUpTo(nullBucketFileId) == 0) {
            return null;
        }

        V result = null;
        OCacheEntry cacheEntry = diskCache.load(nullBucketFileId, 0, false);
        try {
          ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, ODurablePage.TrackMode.NONE, valueSerializer, false);
          result = nullBucket.getValue();
        } finally {
          diskCache.release(cacheEntry);
        }

        return result;
      } else {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        if (keyHashFunction instanceof OMurmurHash3HashFunction) {
          ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
        }

        final long hashCode = keyHashFunction.hashCode(key);

        BucketPath bucketPath = getBucket(hashCode);
        final long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);
        if (bucketPointer == 0) {
            return null;
        }

        long pageIndex = getPageIndex(bucketPointer);
        int fileLevel = getFileLevel(bucketPointer);

        OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
        try {
          final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              ODurablePage.TrackMode.NONE);

          OHashIndexBucket.Entry<K, V> entry = bucket.find(key, hashCode);
          if (entry == null) {
              return null;
          }

          return entry.value;
        } finally {
          diskCache.release(cacheEntry);
        }
      }
    } catch (IOException e) {
      throw new OIndexException("Exception during index value retrieval", e);
    } finally {
      releaseSharedLock();
    }
  }

  public void put(K key, V value) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();

      checkNullSupport(key);

      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      doPut(key, value);

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();
      throw new OIndexException("Error during index update", e);
    } catch (Throwable e) {
      rollback();
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public V remove(K key) {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      checkNullSupport(key);

      int sizeDiff = 0;
      if (key != null) {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);

        if (keyHashFunction instanceof OMurmurHash3HashFunction) {
          ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
        }

        final BucketPath nodePath = getBucket(hashCode);
        final long bucketPointer = directory.getNodePointer(nodePath.nodeIndex, nodePath.itemIndex + nodePath.hashMapOffset);

        final long pageIndex = getPageIndex(bucketPointer);
        final int fileLevel = getFileLevel(bucketPointer);
        final V removed;

        final OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
        cacheEntry.acquireExclusiveLock();
        try {
          final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getTrackMode());
          final int positionIndex = bucket.getIndex(hashCode, key);
          if (positionIndex < 0) {
            endAtomicOperation(false);
            return null;
          }

          removed = bucket.deleteEntry(positionIndex).value;
          sizeDiff--;

          mergeBucketsAfterDeletion(nodePath, bucket);
          cacheEntry.markDirty();
          logPageChanges(bucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
        } finally {
          cacheEntry.releaseExclusiveLock();
          diskCache.release(cacheEntry);
        }

        if (nodePath.parent != null) {
          final int hashMapSize = 1 << nodePath.nodeLocalDepth;

          final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(directory.getNode(nodePath.nodeIndex), hashMapSize);
          if (allMapsContainSameBucket) {
              mergeNodeToParent(nodePath);
          }
        }

        changeSize(sizeDiff);

        endAtomicOperation(false);
        return removed;
      } else {
        if (diskCache.getFilledUpTo(nullBucketFileId) == 0) {
            return null;
        }

        V removed = null;

        OCacheEntry cacheEntry = diskCache.load(nullBucketFileId, 0, false);
        cacheEntry.acquireExclusiveLock();
        try {
          final ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getTrackMode(), valueSerializer, false);

          removed = nullBucket.getValue();
          if (removed != null) {
            nullBucket.removeValue();
            sizeDiff--;
            cacheEntry.markDirty();

            logPageChanges(nullBucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
          }
        } finally {
          cacheEntry.releaseExclusiveLock();
          diskCache.release(cacheEntry);
        }

        changeSize(sizeDiff);

        endAtomicOperation(false);
        return removed;
      }
    } catch (IOException e) {
      rollback();
      throw new OIndexException("Error during index removal", e);
    } catch (Throwable e) {
      rollback();
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void changeSize(int sizeDiff) throws IOException {
    if (sizeDiff != 0) {
      diskCache.loadPinnedPage(hashStateEntry);

      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);

        page.setRecordsCount(page.getRecordsCount() + sizeDiff);
        hashStateEntry.markDirty();

        logPageChanges(page, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        diskCache.release(hashStateEntry);
      }
    }
  }

  public void clear() {
    acquireExclusiveLock();
    try {
      startAtomicOperation();
      diskCache.loadPinnedPage(hashStateEntry);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);

        for (int i = 0; i < HASH_CODE_SIZE; i++) {
          if (!page.isRemoved(i)) {
            diskCache.truncateFile(page.getFileId(i));
            page.setBucketsCount(i, 0);
            page.setTombstoneIndex(i, -1);
          }
        }

        hashStateEntry.markDirty();
        logPageChanges(page, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        diskCache.release(hashStateEntry);
      }

      if (nullKeyIsSupported) {
          diskCache.truncateFile(nullBucketFileId);
      }

      initHashTreeState();

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback();
      throw new OIndexException("Error during hash table clear", e);
    } catch (Throwable e) {
      rollback();
      throw new OSBTreeException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public OHashIndexBucket.Entry<K, V>[] higherEntries(K key) {
    return higherEntries(key, -1);
  }

  public OHashIndexBucket.Entry<K, V>[] higherEntries(K key, int limit) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      if (keyHashFunction instanceof OMurmurHash3HashFunction) {
        ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
      }

      final long hashCode = keyHashFunction.hashCode(key);
      BucketPath bucketPath = getBucket(hashCode);
      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);

        while (bucket.size() == 0 || comparator.compare(bucket.getKey(bucket.size() - 1), key) <= 0) {
          bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
          if (bucketPath == null) {
              return new OHashIndexBucket.Entry[0];
          }

          diskCache.release(cacheEntry);

          final long nextPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

          fileLevel = getFileLevel(nextPointer);
          pageIndex = getPageIndex(nextPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);
          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);
        }

        final int index = bucket.getIndex(hashCode, key);
        final int startIndex;
        if (index >= 0) {
            startIndex = index + 1;
        } else {
            startIndex = -index - 1;
        }

        final int endIndex;
        if (limit <= 0) {
            endIndex = bucket.size();
        } else {
            endIndex = Math.min(bucket.size(), startIndex + limit);
        }

        return convertBucketToEntries(bucket, startIndex, endIndex);
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data retrieval", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  public void load(String name, OType[] keyTypes, OAbstractPaginatedStorage storageLocal, boolean nullKeyIsSupported) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.keyTypes = keyTypes;
      this.nullKeyIsSupported = nullKeyIsSupported;

      diskCache = storage.getDiskCache();

      this.name = name;

      init(storage);

      fileStateId = diskCache.openFile(name + metadataConfigurationFileExtension);
      hashStateEntry = diskCache.load(fileStateId, 0, true);

      directory = new OHashTableDirectory(treeStateFileExtension, name, durableInNonTxMode, storage);
      directory.open();

      diskCache.pinPage(hashStateEntry);
      try {
        OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, ODurablePage.TrackMode.NONE,
            false);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance()
            .getObjectSerializer(page.getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            page.getValueSerializerId());

        for (int i = 0; i < HASH_CODE_SIZE; i++) {
            if (!page.isRemoved(i)) {
                diskCache.openFile(page.getFileId(i));
            }
        }
      } finally {
        diskCache.release(hashStateEntry);
      }

      if (nullKeyIsSupported) {
          nullBucketFileId = diskCache.openFile(name + nullBucketFileExtension);
      }
    } catch (IOException e) {
      throw new OIndexException("Exception during hash table loading", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void deleteWithoutLoad(String name, OAbstractPaginatedStorage storageLocal) {
    acquireExclusiveLock();
    try {
      storage = storageLocal;

      final ODiskCache diskCache = storage.getDiskCache();

      if (diskCache.exists(name + metadataConfigurationFileExtension)) {
        fileStateId = diskCache.openFile(name + metadataConfigurationFileExtension);
        hashStateEntry = diskCache.load(fileStateId, 0, true);
        try {
          OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
              ODurablePage.TrackMode.NONE, false);
          for (int i = 0; i < HASH_CODE_SIZE; i++) {
            if (!metadataPage.isRemoved(i)) {
              final long fileId = metadataPage.getFileId(i);
              if (diskCache.exists(fileId)) {
                diskCache.openFile(fileId);
                diskCache.deleteFile(fileId);
              }
            }
          }
        } finally {
          diskCache.release(hashStateEntry);
        }

        diskCache.deleteFile(fileStateId);

        directory = new OHashTableDirectory(treeStateFileExtension, name, durableInNonTxMode, storage);
        directory.deleteWithoutOpen();

        if (diskCache.exists(name + nullBucketFileExtension)) {
          final long nullBucketId = diskCache.openFile(name + nullBucketFileExtension);
          diskCache.deleteFile(nullBucketId);
        }
      }
    } catch (IOException ioe) {
      throw new OIndexException("Can not delete hash table with name " + name, ioe);
    } finally {
      releaseExclusiveLock();
    }
  }

  private OHashIndexBucket.Entry<K, V>[] convertBucketToEntries(final OHashIndexBucket<K, V> bucket, int startIndex, int endIndex) {
    final OHashIndexBucket.Entry<K, V>[] entries = new OHashIndexBucket.Entry[endIndex - startIndex];
    final Iterator<OHashIndexBucket.Entry<K, V>> iterator = bucket.iterator(startIndex);

    for (int i = 0, k = startIndex; k < endIndex; i++, k++) {
        entries[i] = iterator.next();
    }

    return entries;
  }

  private BucketPath nextBucketToFind(final BucketPath bucketPath, int bucketDepth) throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    BucketPath currentNode = bucketPath;
    int nodeLocalDepth = directory.getNodeLocalDepth(bucketPath.nodeIndex);

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex) == bucketPath.nodeLocalDepth;

    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
        assert directory.getNodeLocalDepth(currentNode.nodeIndex) == currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);

    final BucketPath bucketPathToFind;
    final int globalIndex = firstStartIndex + interval + currentNode.hashMapOffset;
    if (globalIndex >= MAX_LEVEL_SIZE) {
        bucketPathToFind = nextLevelUp(currentNode);
    } else {
      final int hashMapSize = 1 << currentNode.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind = new BucketPath(currentNode.parent, hashMapOffset, startIndex, currentNode.nodeIndex,
          currentNode.nodeLocalDepth, currentNode.nodeGlobalDepth);
    }

    return nextNonEmptyNode(bucketPathToFind);
  }

  private BucketPath nextNonEmptyNode(BucketPath bucketPath) throws IOException {
    nextBucketLoop: while (bucketPath != null) {
      final long[] node = directory.getNode(bucketPath.nodeIndex);
      final int startIndex = bucketPath.itemIndex + bucketPath.hashMapOffset;
      final int endIndex = MAX_LEVEL_SIZE;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];

        if (position > 0) {
          final int hashMapSize = 1 << bucketPath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new BucketPath(bucketPath.parent, hashMapOffset, itemIndex, bucketPath.nodeIndex, bucketPath.nodeLocalDepth,
              bucketPath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final BucketPath parent = new BucketPath(bucketPath.parent, 0, i, bucketPath.nodeIndex, bucketPath.nodeLocalDepth,
              bucketPath.nodeGlobalDepth);

          final int childLocalDepth = directory.getNodeLocalDepth(childNodeIndex);
          bucketPath = new BucketPath(parent, childItemOffset, 0, childNodeIndex, childLocalDepth, bucketPath.nodeGlobalDepth
              + childLocalDepth);

          continue nextBucketLoop;
        }
      }

      bucketPath = nextLevelUp(bucketPath);
    }

    return null;
  }

  private BucketPath nextLevelUp(BucketPath bucketPath) throws IOException {
    if (bucketPath.parent == null) {
        return null;
    }

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex) == bucketPath.nodeLocalDepth;

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final BucketPath parent = bucketPath.parent;

    if (parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);
    }

    final int nextParentIndex = ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize + 1) * pointersSize + MAX_LEVEL_SIZE / 2;
    if (nextParentIndex < MAX_LEVEL_SIZE) {
        return new BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);
    }

    return nextLevelUp(new BucketPath(parent.parent, 0, MAX_LEVEL_SIZE - 1, parent.nodeIndex, parent.nodeLocalDepth,
        parent.nodeGlobalDepth));
  }

  public OHashIndexBucket.Entry<K, V>[] ceilingEntries(K key) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      if (keyHashFunction instanceof OMurmurHash3HashFunction) {
        ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
      }

      final long hashCode = keyHashFunction.hashCode(key);
      BucketPath bucketPath = getBucket(hashCode);

      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);
        while (bucket.size() == 0) {
          bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
          if (bucketPath == null) {
              return new OHashIndexBucket.Entry[0];
          }

          diskCache.release(cacheEntry);
          final long nextPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

          fileLevel = getFileLevel(nextPointer);
          pageIndex = getPageIndex(nextPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);
          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);
        }

        final int index = bucket.getIndex(hashCode, key);
        final int startIndex;
        if (index >= 0) {
            startIndex = index;
        } else {
            startIndex = -index - 1;
        }

        final int endIndex = bucket.size();
        return convertBucketToEntries(bucket, startIndex, endIndex);
      } finally {
        diskCache.release(cacheEntry);
      }

    } catch (IOException ioe) {
      throw new OIndexException("Error during data retrieval", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  public OHashIndexBucket.Entry<K, V> firstEntry() {
    acquireSharedLock();
    try {
      BucketPath bucketPath = getBucket(HASH_CODE_MIN_VALUE);
      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);

        while (bucket.size() == 0) {
          bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
          if (bucketPath == null) {
              return null;
          }

          diskCache.release(cacheEntry);
          final long nextPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

          fileLevel = getFileLevel(nextPointer);
          pageIndex = getPageIndex(nextPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);
          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);
        }

        return bucket.getEntry(0);
      } finally {
        diskCache.release(cacheEntry);
      }

    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  public OHashIndexBucket.Entry<K, V> lastEntry() {
    acquireSharedLock();
    try {
      BucketPath bucketPath = getBucket(HASH_CODE_MAX_VALUE);
      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);

        while (bucket.size() == 0) {
          final BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
          if (prevBucketPath == null) {
              return null;
          }

          diskCache.release(cacheEntry);
          final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
              + prevBucketPath.hashMapOffset);

          fileLevel = getFileLevel(prevPointer);
          pageIndex = getPageIndex(prevPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);
          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);

          bucketPath = prevBucketPath;
        }

        return bucket.getEntry(bucket.size() - 1);
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  public OHashIndexBucket.Entry<K, V>[] lowerEntries(K key) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      if (keyHashFunction instanceof OMurmurHash3HashFunction) {
        ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
      }

      final long hashCode = keyHashFunction.hashCode(key);
      BucketPath bucketPath = getBucket(hashCode);

      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);
        while (bucket.size() == 0 || comparator.compare(bucket.getKey(0), key) >= 0) {
          final BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
          if (prevBucketPath == null) {
              return new OHashIndexBucket.Entry[0];
          }

          diskCache.release(cacheEntry);

          final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
              + prevBucketPath.hashMapOffset);

          fileLevel = getFileLevel(prevPointer);
          pageIndex = getPageIndex(prevPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);
          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);

          bucketPath = prevBucketPath;
        }

        final int startIndex = 0;
        final int index = bucket.getIndex(hashCode, key);

        final int endIndex;
        if (index >= 0) {
            endIndex = index;
        } else {
            endIndex = -index - 1;
        }

        return convertBucketToEntries(bucket, startIndex, endIndex);
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  public OHashIndexBucket.Entry<K, V>[] floorEntries(K key) {
    acquireSharedLock();
    try {
      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      if (keyHashFunction instanceof OMurmurHash3HashFunction) {
        ((OMurmurHash3HashFunction) keyHashFunction).setValueSerializer(keySerializer);
      }

      final long hashCode = keyHashFunction.hashCode(key);
      BucketPath bucketPath = getBucket(hashCode);

      long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

      int fileLevel = getFileLevel(bucketPointer);
      long pageIndex = getPageIndex(bucketPointer);

      OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      try {
        OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            ODurablePage.TrackMode.NONE);
        while (bucket.size() == 0) {
          final BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
          if (prevBucketPath == null) {
              return new OHashIndexBucket.Entry[0];
          }

          diskCache.release(cacheEntry);

          final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
              + prevBucketPath.hashMapOffset);

          fileLevel = getFileLevel(prevPointer);
          pageIndex = getPageIndex(prevPointer);

          cacheEntry = loadPageEntry(pageIndex, fileLevel);

          bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, ODurablePage.TrackMode.NONE);

          bucketPath = prevBucketPath;
        }

        final int startIndex = 0;
        final int index = bucket.getIndex(hashCode, key);

        final int endIndex;
        if (index >= 0) {
            endIndex = index + 1;
        } else {
            endIndex = -index - 1;
        }

        return convertBucketToEntries(bucket, startIndex, endIndex);
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      releaseSharedLock();
    }
  }

  private BucketPath prevBucketToFind(final BucketPath bucketPath, int bucketDepth) throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    BucketPath currentBucket = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentBucket = bucketPath.parent;
        nodeLocalDepth = currentBucket.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentBucket.nodeGlobalDepth - nodeLocalDepth);
    final int firstStartIndex = currentBucket.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int globalIndex = firstStartIndex + currentBucket.hashMapOffset - 1;

    final BucketPath bucketPathToFind;
    if (globalIndex < 0) {
        bucketPathToFind = prevLevelUp(bucketPath);
    } else {
      final int hashMapSize = 1 << currentBucket.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind = new BucketPath(currentBucket.parent, hashMapOffset, startIndex, currentBucket.nodeIndex,
          currentBucket.nodeLocalDepth, currentBucket.nodeGlobalDepth);
    }

    return prevNonEmptyNode(bucketPathToFind);
  }

  private BucketPath prevNonEmptyNode(BucketPath nodePath) throws IOException {
    prevBucketLoop: while (nodePath != null) {
      final long[] node = directory.getNode(nodePath.nodeIndex);
      final int startIndex = 0;
      final int endIndex = nodePath.itemIndex + nodePath.hashMapOffset;

      for (int i = endIndex; i >= startIndex; i--) {
        final long position = node[i];
        if (position > 0) {
          final int hashMapSize = 1 << nodePath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new BucketPath(nodePath.parent, hashMapOffset, itemIndex, nodePath.nodeIndex, nodePath.nodeLocalDepth,
              nodePath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;
          final int nodeLocalDepth = directory.getNodeLocalDepth(childNodeIndex);
          final int endChildIndex = (1 << nodeLocalDepth) - 1;

          final BucketPath parent = new BucketPath(nodePath.parent, 0, i, nodePath.nodeIndex, nodePath.nodeLocalDepth,
              nodePath.nodeGlobalDepth);
          nodePath = new BucketPath(parent, childItemOffset, endChildIndex, childNodeIndex, nodeLocalDepth, parent.nodeGlobalDepth
              + nodeLocalDepth);
          continue prevBucketLoop;
        }
      }

      nodePath = prevLevelUp(nodePath);
    }

    return null;
  }

  private BucketPath prevLevelUp(BucketPath bucketPath) {
    if (bucketPath.parent == null) {
        return null;
    }

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final BucketPath parent = bucketPath.parent;

    if (parent.itemIndex > MAX_LEVEL_SIZE / 2) {
      final int prevParentIndex = ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize + MAX_LEVEL_SIZE / 2 - 1;
      return new BucketPath(parent.parent, 0, prevParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);
    }

    final int prevParentIndex = (parent.itemIndex / pointersSize) * pointersSize - 1;
    if (prevParentIndex >= 0) {
        return new BucketPath(parent.parent, 0, prevParentIndex, parent.nodeIndex, parent.nodeLocalDepth, parent.nodeGlobalDepth);
    }

    return prevLevelUp(new BucketPath(parent.parent, 0, 0, parent.nodeIndex, parent.nodeLocalDepth, -1));
  }

  public long size() {
    acquireSharedLock();
    try {
      diskCache.loadPinnedPage(hashStateEntry);
      try {
        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
            ODurablePage.TrackMode.NONE, false);
        return metadataPage.getRecordsCount();
      } finally {
        diskCache.release(hashStateEntry);
      }
    } catch (IOException e) {
      throw new OIndexException("Error during index size request.", e);
    } finally {
      releaseSharedLock();
    }
  }

  public void close() {
    acquireExclusiveLock();
    try {
      flush();

      directory.close();
      diskCache.loadPinnedPage(hashStateEntry);

      try {
        for (int i = 0; i < HASH_CODE_SIZE; i++) {
          OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
              ODurablePage.TrackMode.NONE, false);
          if (!metadataPage.isRemoved(i)) {
            diskCache.closeFile(metadataPage.getFileId(i));
          }
        }
      } finally {
        diskCache.release(hashStateEntry);
      }

      diskCache.closeFile(fileStateId);
    } catch (IOException e) {
      throw new OIndexException("Error during hash table close", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      diskCache.loadPinnedPage(hashStateEntry);
      try {
        for (int i = 0; i < HASH_CODE_SIZE; i++) {
          OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
              ODurablePage.TrackMode.NONE, false);
          if (!metadataPage.isRemoved(i)) {
            diskCache.deleteFile(metadataPage.getFileId(i));
          }
        }
      } finally {
        diskCache.release(hashStateEntry);
      }

      directory.delete();
      diskCache.deleteFile(fileStateId);

      if (nullKeyIsSupported) {
          diskCache.deleteFile(nullBucketFileId);
      }

    } catch (IOException e) {
      throw new OIndexException("Exception during index deletion", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void mergeNodeToParent(BucketPath nodePath) throws IOException {
    final int startIndex = findParentNodeStartIndex(nodePath);
    final int localNodeDepth = nodePath.nodeLocalDepth;
    final int hashMapSize = 1 << localNodeDepth;

    final int parentIndex = nodePath.parent.nodeIndex;
    for (int i = 0, k = startIndex; i < MAX_LEVEL_SIZE; i += hashMapSize, k++) {
      directory.setNodePointer(parentIndex, k, directory.getNodePointer(nodePath.nodeIndex, i));
    }

    directory.deleteNode(nodePath.nodeIndex);

    if (nodePath.parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth = directory.getMaxLeftChildDepth(parentIndex);
      if (maxChildDepth == localNodeDepth) {
          directory.setMaxLeftChildDepth(parentIndex, (byte) getMaxLevelDepth(parentIndex, 0, MAX_LEVEL_SIZE / 2));
      }
    } else {
      final int maxChildDepth = directory.getMaxRightChildDepth(parentIndex);
      if (maxChildDepth == localNodeDepth) {
          directory.setMaxRightChildDepth(parentIndex, (byte) getMaxLevelDepth(parentIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE));
      }
    }
  }

  private void mergeBucketsAfterDeletion(BucketPath nodePath, OHashIndexBucket<K, V> bucket) throws IOException {
    final int bucketDepth = bucket.getDepth();

    if (bucket.getContentSize() > OHashIndexBucket.MAX_BUCKET_SIZE_BYTES * MERGE_THRESHOLD) {
        return;
    }

    if (bucketDepth - MAX_LEVEL_DEPTH < 1) {
        return;
    }

    int offset = nodePath.nodeGlobalDepth - (bucketDepth - 1);
    BucketPath currentNode = nodePath;
    int nodeLocalDepth = nodePath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = nodePath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff - 1));

    int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    final OHashIndexBucket<K, V> buddyBucket;

    int buddyLevel;
    long buddyIndex;
    long buddyPointer;

    if ((currentNode.itemIndex >>> (nodeLocalDepth - diff - 1) & 1) == 1) {
      buddyPointer = directory.getNodePointer(currentNode.nodeIndex, firstStartIndex + currentNode.hashMapOffset);

      while (buddyPointer < 0) {
        final int nodeIndex = (int) ((buddyPointer & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPointer & 0xFF;

        buddyPointer = directory.getNodePointer(nodeIndex, itemOffset);
      }

      assert buddyPointer > 0;

      buddyLevel = getFileLevel(buddyPointer);
      buddyIndex = getPageIndex(buddyPointer);
    } else {
      buddyPointer = directory.getNodePointer(currentNode.nodeIndex, secondStartIndex + currentNode.hashMapOffset);

      while (buddyPointer < 0) {
        final int nodeIndex = (int) ((buddyPointer & Long.MAX_VALUE) >> 8);
        final int itemOffset = (int) buddyPointer & 0xFF;

        buddyPointer = directory.getNodePointer(nodeIndex, itemOffset);
      }

      assert buddyPointer > 0;

      buddyLevel = getFileLevel(buddyPointer);
      buddyIndex = getPageIndex(buddyPointer);
    }

    OCacheEntry buddyCacheEntry = loadPageEntry(buddyIndex, buddyLevel);
    buddyCacheEntry.acquireExclusiveLock();
    try {
      diskCache.loadPinnedPage(hashStateEntry);
      hashStateEntry.acquireExclusiveLock();
      try {
        buddyBucket = new OHashIndexBucket<K, V>(buddyCacheEntry, keySerializer, valueSerializer, keyTypes, getTrackMode());

        if (buddyBucket.getDepth() != bucketDepth) {
            return;
        }

        if (bucket.mergedSize(buddyBucket) >= OHashIndexBucket.MAX_BUCKET_SIZE_BYTES) {
            return;
        }

        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);
        hashStateEntry.markDirty();

        metadataPage.setBucketsCount(buddyLevel, metadataPage.getBucketsCount(buddyLevel) - 2);

        int newBuddyLevel = buddyLevel - 1;
        long newBuddyIndex = buddyBucket.getSplitHistory(newBuddyLevel);

        metadataPage.setBucketsCount(buddyLevel, metadataPage.getBucketsCount(buddyLevel) + 1);

        final OCacheEntry newBuddyCacheEntry = loadPageEntry(newBuddyIndex, newBuddyLevel);
        newBuddyCacheEntry.acquireExclusiveLock();
        try {
          final OHashIndexBucket<K, V> newBuddyBucket = new OHashIndexBucket<K, V>(bucketDepth - 1, newBuddyCacheEntry,
              keySerializer, valueSerializer, keyTypes, getTrackMode());

          for (OHashIndexBucket.Entry<K, V> entry : buddyBucket) {
              newBuddyBucket.appendEntry(entry.hashCode, entry.key, entry.value);
          }

          for (OHashIndexBucket.Entry<K, V> entry : bucket) {
              newBuddyBucket.addEntry(entry.hashCode, entry.key, entry.value);
          }

          logPageChanges(newBuddyBucket, newBuddyCacheEntry.getFileId(), newBuddyCacheEntry.getPageIndex(), false);
        } finally {
          newBuddyCacheEntry.markDirty();
          newBuddyCacheEntry.releaseExclusiveLock();

          diskCache.release(newBuddyCacheEntry);
        }

        final long bucketPointer = directory.getNodePointer(nodePath.nodeIndex, nodePath.itemIndex + nodePath.hashMapOffset);
        final long bucketIndex = getPageIndex(bucketPointer);

        final long newBuddyPointer = createBucketPointer(buddyIndex, buddyLevel);

        for (int i = firstStartIndex; i < secondEndIndex; i++) {
            updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, newBuddyPointer);
        }

        if (metadataPage.getBucketsCount(buddyLevel) > 0) {
          final long newTombstoneIndex;
          if (bucketIndex < buddyIndex) {
            bucket.setNextRemovedBucketPair(metadataPage.getTombstoneIndex(buddyLevel));

            newTombstoneIndex = bucketIndex;
          } else {
            buddyBucket.setNextRemovedBucketPair(metadataPage.getTombstoneIndex(buddyLevel));
            buddyCacheEntry.markDirty();

            logPageChanges(buddyBucket, buddyCacheEntry.getFileId(), buddyCacheEntry.getPageIndex(), false);
            newTombstoneIndex = buddyIndex;
          }

          metadataPage.setTombstoneIndex(buddyLevel, newTombstoneIndex);
        } else {
            metadataPage.setTombstoneIndex(buddyLevel, -1);
        }

        logPageChanges(metadataPage, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        diskCache.release(hashStateEntry);
      }
    } finally {
      buddyCacheEntry.releaseExclusiveLock();
      diskCache.release(buddyCacheEntry);
    }
  }

  public void flush() {
    acquireExclusiveLock();
    try {
      diskCache.loadPinnedPage(hashStateEntry);
      try {
        for (int i = 0; i < HASH_CODE_SIZE; i++) {
          OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
              ODurablePage.TrackMode.NONE, false);
          if (!metadataPage.isRemoved(i)) {
              diskCache.flushFile(metadataPage.getFileId(i));
          }
        }
      } finally {
        diskCache.release(hashStateEntry);
      }

      diskCache.flushFile(fileStateId);
      directory.flush();

      if (nullKeyIsSupported) {
          diskCache.flushFile(nullBucketFileId);
      }
    } catch (IOException e) {
      throw new OIndexException("Error during hash table flush", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void doPut(K key, V value) throws IOException {
    int sizeDiff = 0;

    if (key == null) {
      boolean isNew;
      OCacheEntry cacheEntry;
      if (diskCache.getFilledUpTo(nullBucketFileId) == 0) {
        cacheEntry = diskCache.allocateNewPage(nullBucketFileId);
        isNew = true;
      } else {
        cacheEntry = diskCache.load(nullBucketFileId, 0, false);
        isNew = false;
      }

      cacheEntry.acquireExclusiveLock();
      try {
        ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getTrackMode(), valueSerializer, isNew);
        if (nullBucket.getValue() != null) {
            sizeDiff--;
        }

        nullBucket.setValue(value);
        sizeDiff++;
        cacheEntry.markDirty();

        logPageChanges(nullBucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), isNew);
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      changeSize(sizeDiff);
    } else {
      final long hashCode = keyHashFunction.hashCode(key);

      final BucketPath bucketPath = getBucket(hashCode);
      final long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);
      if (bucketPointer == 0) {
          throw new IllegalStateException("In this version of hash table buckets are added through split only.");
      }

      final long pageIndex = getPageIndex(bucketPointer);
      final int fileLevel = getFileLevel(bucketPointer);

      final OCacheEntry cacheEntry = loadPageEntry(pageIndex, fileLevel);
      cacheEntry.acquireExclusiveLock();
      try {
        final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            getTrackMode());
        final int index = bucket.getIndex(hashCode, key);

        if (index > -1) {
          final int updateResult = bucket.updateEntry(index, value);
          if (updateResult == 0) {
            changeSize(sizeDiff);
            return;
          }

          if (updateResult == 1) {
            cacheEntry.markDirty();
            logPageChanges(bucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
            changeSize(sizeDiff);
            return;
          }

          assert updateResult == -1;

          bucket.deleteEntry(index);
          sizeDiff--;
        }

        if (bucket.addEntry(hashCode, key, value)) {
          cacheEntry.markDirty();
          logPageChanges(bucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);

          sizeDiff++;

          changeSize(sizeDiff);
          return;
        }

        final BucketSplitResult splitResult = splitBucket(bucket, fileLevel, pageIndex);

        final long updatedBucketPointer = splitResult.updatedBucketPointer;
        final long newBucketPointer = splitResult.newBucketPointer;
        final int bucketDepth = splitResult.newDepth;

        if (bucketDepth <= bucketPath.nodeGlobalDepth) {
          updateNodeAfterBucketSplit(bucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
        } else {
          if (bucketPath.nodeLocalDepth < MAX_LEVEL_DEPTH) {
            final NodeSplitResult nodeSplitResult = splitNode(bucketPath);

            assert !(nodeSplitResult.allLeftHashMapsEqual && nodeSplitResult.allRightHashMapsEqual);

            final long[] newNode = nodeSplitResult.newNode;

            final int nodeLocalDepth = bucketPath.nodeLocalDepth + 1;
            final int hashMapSize = 1 << nodeLocalDepth;

            assert nodeSplitResult.allRightHashMapsEqual == checkAllMapsContainSameBucket(newNode, hashMapSize);

            int newNodeIndex = -1;
            if (!nodeSplitResult.allRightHashMapsEqual || bucketPath.itemIndex >= MAX_LEVEL_SIZE / 2) {
                newNodeIndex = directory.addNewNode((byte) 0, (byte) 0, (byte) nodeLocalDepth, newNode);
            }

            final int updatedItemIndex = bucketPath.itemIndex << 1;
            final int updatedOffset = bucketPath.hashMapOffset << 1;
            final int updatedGlobalDepth = bucketPath.nodeGlobalDepth + 1;

            boolean allLeftHashMapsEqual = nodeSplitResult.allLeftHashMapsEqual;
            boolean allRightHashMapsEqual = nodeSplitResult.allRightHashMapsEqual;

            if (updatedOffset < MAX_LEVEL_SIZE) {
              allLeftHashMapsEqual = false;
              final BucketPath updatedBucketPath = new BucketPath(bucketPath.parent, updatedOffset, updatedItemIndex,
                  bucketPath.nodeIndex, nodeLocalDepth, updatedGlobalDepth);
              updateNodeAfterBucketSplit(updatedBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
            } else {
              allRightHashMapsEqual = false;
              final BucketPath newBucketPath = new BucketPath(bucketPath.parent, updatedOffset - MAX_LEVEL_SIZE, updatedItemIndex,
                  newNodeIndex, nodeLocalDepth, updatedGlobalDepth);
              updateNodeAfterBucketSplit(newBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
            }

            updateNodesAfterSplit(bucketPath, bucketPath.nodeIndex, newNode, nodeLocalDepth, hashMapSize, allLeftHashMapsEqual,
                allRightHashMapsEqual, newNodeIndex);

            if (allLeftHashMapsEqual) {
                directory.deleteNode(bucketPath.nodeIndex);
            }
          } else {
            addNewLevelNode(bucketPath, bucketPath.nodeIndex, newBucketPointer, updatedBucketPointer);
          }
        }
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      changeSize(sizeDiff);
      doPut(key, value);
    }

  }

  private void checkNullSupport(K key) {
    if (key == null && !nullKeyIsSupported) {
        throw new OIndexException("Null keys are not supported.");
    }
  }

  private void updateNodesAfterSplit(BucketPath bucketPath, int nodeIndex, long[] newNode, int nodeLocalDepth, int hashMapSize,
      boolean allLeftHashMapEquals, boolean allRightHashMapsEquals, int newNodeIndex) throws IOException {

    final int startIndex = findParentNodeStartIndex(bucketPath);

    final int parentNodeIndex = bucketPath.parent.nodeIndex;
    assert assertParentNodeStartIndex(bucketPath, directory.getNode(parentNodeIndex), startIndex);

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);
    if (allLeftHashMapEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = directory.getNodePointer(nodeIndex, i * hashMapSize);
        directory.setNodePointer(parentNodeIndex, startIndex + i, position);
      }
    } else {
      for (int i = 0; i < pointersSize; i++) {
          directory.setNodePointer(parentNodeIndex, startIndex + i, (bucketPath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE);
      }
    }

    if (allRightHashMapsEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = newNode[i * hashMapSize];
        directory.setNodePointer(parentNodeIndex, startIndex + pointersSize + i, position);
      }
    } else {
      for (int i = 0; i < pointersSize; i++) {
          directory.setNodePointer(parentNodeIndex, startIndex + pointersSize + i, (newNodeIndex << 8) | (i * hashMapSize)
                  | Long.MIN_VALUE);
      }
    }

    updateMaxChildDepth(bucketPath.parent, bucketPath.nodeLocalDepth + 1);
  }

  private void updateMaxChildDepth(BucketPath parentPath, int childDepth) throws IOException {
    if (parentPath == null) {
        return;
    }

    if (parentPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth = directory.getMaxLeftChildDepth(parentPath.nodeIndex);
      if (childDepth > maxChildDepth) {
          directory.setMaxLeftChildDepth(parentPath.nodeIndex, (byte) childDepth);
      }
    } else {
      final int maxChildDepth = directory.getMaxRightChildDepth(parentPath.nodeIndex);
      if (childDepth > maxChildDepth) {
          directory.setMaxRightChildDepth(parentPath.nodeIndex, (byte) childDepth);
      }
    }
  }

  private boolean assertParentNodeStartIndex(BucketPath bucketPath, long[] parentNode, int calculatedIndex) {
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++) {
        if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == bucketPath.nodeIndex) {
            startIndex = i;
            break;
        }
    }

    return startIndex == calculatedIndex;
  }

  private int findParentNodeStartIndex(BucketPath bucketPath) {
    final BucketPath parentBucketPath = bucketPath.parent;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - bucketPath.nodeLocalDepth);

    if (parentBucketPath.itemIndex < MAX_LEVEL_SIZE / 2) {
        return (parentBucketPath.itemIndex / pointersSize) * pointersSize;
    }

    return ((parentBucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize + MAX_LEVEL_SIZE / 2;
  }

  private void addNewLevelNode(BucketPath bucketPath, int nodeIndex, long newBucketPointer, long updatedBucketPointer)
      throws IOException {
    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (bucketPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxDepth = directory.getMaxLeftChildDepth(bucketPath.nodeIndex);

      assert getMaxLevelDepth(bucketPath.nodeIndex, 0, MAX_LEVEL_SIZE / 2) == maxDepth;

      if (maxDepth > 0) {
          newNodeDepth = maxDepth;
      } else {
          newNodeDepth = 1;
      }

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = (bucketPath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = directory.getMaxRightChildDepth(bucketPath.nodeIndex);
      assert getMaxLevelDepth(bucketPath.nodeIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE) == maxDepth;
      if (maxDepth > 0) {
          newNodeDepth = maxDepth;
      } else {
          newNodeDepth = 1;
      }

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = ((bucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / mapInterval) * mapInterval + MAX_LEVEL_SIZE / 2;
    }

    final int newNodeIndex = directory.addNewNode((byte) 0, (byte) 0, (byte) newNodeDepth, new long[MAX_LEVEL_SIZE]);

    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long bucketPointer = directory.getNodePointer(nodeIndex, nodeOffset);

      if (nodeOffset != bucketPath.itemIndex) {
        for (int n = i << newNodeDepth; n < (i + 1) << newNodeDepth; n++) {
            directory.setNodePointer(newNodeIndex, n, bucketPointer);
        }
      } else {
        for (int n = i << newNodeDepth; n < (2 * i + 1) << (newNodeDepth - 1); n++) {
            directory.setNodePointer(newNodeIndex, n, updatedBucketPointer);
        }

        for (int n = (2 * i + 1) << (newNodeDepth - 1); n < (i + 1) << newNodeDepth; n++) {
            directory.setNodePointer(newNodeIndex, n, newBucketPointer);
        }
      }

      directory.setNodePointer(nodeIndex, nodeOffset, (newNodeIndex << 8) | (i * mapSize) | Long.MIN_VALUE);
    }

    updateMaxChildDepth(bucketPath, newNodeDepth);
  }

  private int getMaxLevelDepth(int nodeIndex, int start, int end) throws IOException {
    int currentIndex = -1;
    int maxDepth = 0;

    for (int i = start; i < end; i++) {
      final long nodePosition = directory.getNodePointer(nodeIndex, i);
      if (nodePosition >= 0) {
          continue;
      }

      final int index = (int) ((nodePosition & Long.MAX_VALUE) >>> 8);
      if (index == currentIndex) {
          continue;
      }

      currentIndex = index;

      final int nodeLocalDepth = directory.getNodeLocalDepth(index);
      if (maxDepth < nodeLocalDepth) {
          maxDepth = nodeLocalDepth;
      }
    }

    return maxDepth;
  }

  private void updateNodeAfterBucketSplit(BucketPath bucketPath, int bucketDepth, long newBucketPointer, long updatedBucketPointer)
      throws IOException {
    int offset = bucketPath.nodeGlobalDepth - (bucketDepth - 1);
    BucketPath currentNode = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff - 1));
    final int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    for (int i = firstStartIndex; i < firstEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, updatedBucketPointer);
    }

    for (int i = secondStartIndex; i < secondEndIndex; i++) {
        updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, newBucketPointer);
    }
  }

  private boolean checkAllMapsContainSameBucket(long[] newNode, int hashMapSize) {
    int n = 0;
    boolean allHashMapsEquals = true;
    while (n < newNode.length) {
      boolean allHashBucketEquals = true;
      for (int i = 0; i < hashMapSize - 1; i++) {
        if (newNode[i + n] != newNode[i + n + 1]) {
          allHashBucketEquals = false;
          break;
        }
      }
      n += hashMapSize;
      if (!allHashBucketEquals) {
        allHashMapsEquals = false;
        break;
      }
    }

    assert assertAllNodesAreFilePointers(allHashMapsEquals, newNode, hashMapSize);

    return allHashMapsEquals;
  }

  private boolean assertAllNodesAreFilePointers(boolean allHashMapsEquals, long[] newNode, int hashMapSize) {
    if (allHashMapsEquals) {
      int n = 0;
      while (n < newNode.length) {
        for (int i = 0; i < hashMapSize; i++) {
          if (newNode[i] < 0) {
            return false;
          }
        }
        n += hashMapSize;
      }
    }

    return true;
  }

  private NodeSplitResult splitNode(BucketPath bucketPath) throws IOException {
    final long[] newNode = new long[MAX_LEVEL_SIZE];
    final int hashMapSize = 1 << (bucketPath.nodeLocalDepth + 1);

    boolean hashMapItemsAreEqual = true;
    boolean allLeftItemsAreEqual;
    boolean allRightItemsAreEqual;

    int mapCounter = 0;
    long firstPosition = -1;

    long[] node = directory.getNode(bucketPath.nodeIndex);

    for (int i = MAX_LEVEL_SIZE / 2; i < MAX_LEVEL_SIZE; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0) {
          firstPosition = position;
      }

      newNode[2 * (i - MAX_LEVEL_SIZE / 2)] = position;
      newNode[2 * (i - MAX_LEVEL_SIZE / 2) + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;
        mapCounter += 2;

        if (mapCounter >= hashMapSize) {
            mapCounter = 0;
        }
      }
    }

    mapCounter = 0;
    allRightItemsAreEqual = hashMapItemsAreEqual;

    hashMapItemsAreEqual = true;
    final long[] updatedNode = new long[node.length];
    for (int i = 0; i < MAX_LEVEL_SIZE / 2; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0) {
          firstPosition = position;
      }

      updatedNode[2 * i] = position;
      updatedNode[2 * i + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;

        mapCounter += 2;

        if (mapCounter >= hashMapSize) {
            mapCounter = 0;
        }
      }
    }

    allLeftItemsAreEqual = hashMapItemsAreEqual;

    directory.setNode(bucketPath.nodeIndex, updatedNode);
    directory.setNodeLocalDepth(bucketPath.nodeIndex, (byte) (directory.getNodeLocalDepth(bucketPath.nodeIndex) + 1));

    return new NodeSplitResult(newNode, allLeftItemsAreEqual, allRightItemsAreEqual);
  }

  private void splitBucketContent(OHashIndexBucket<K, V> bucket, OHashIndexBucket<K, V> updatedBucket,
      OHashIndexBucket<K, V> newBucket, int newBucketDepth) throws IOException {
    assert checkBucketDepth(bucket);

    for (OHashIndexBucket.Entry<K, V> entry : bucket) {
      if (((keyHashFunction.hashCode(entry.key) >>> (HASH_CODE_SIZE - newBucketDepth)) & 1) == 0) {
          updatedBucket.appendEntry(entry.hashCode, entry.key, entry.value);
      } else {
          newBucket.appendEntry(entry.hashCode, entry.key, entry.value);
      }
    }

    updatedBucket.setDepth(newBucketDepth);
    newBucket.setDepth(newBucketDepth);

    assert checkBucketDepth(updatedBucket);
    assert checkBucketDepth(newBucket);
  }

  private BucketSplitResult splitBucket(OHashIndexBucket<K, V> bucket, int fileLevel, long pageIndex) throws IOException {
    int bucketDepth = bucket.getDepth();
    int newBucketDepth = bucketDepth + 1;

    final int newFileLevel = newBucketDepth - MAX_LEVEL_DEPTH;
    diskCache.loadPinnedPage(hashStateEntry);

    hashStateEntry.acquireExclusiveLock();
    try {
      OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);

      hashStateEntry.markDirty();

      if (metadataPage.isRemoved(newFileLevel)) {
          createFileMetadata(newFileLevel, metadataPage);
      }

      final long tombstoneIndex = metadataPage.getTombstoneIndex(newFileLevel);

      final long updatedBucketIndex;

      if (tombstoneIndex >= 0) {
        final OCacheEntry tombstoneCacheEntry = loadPageEntry(tombstoneIndex, newFileLevel);
        try {
          final OHashIndexBucket<K, V> tombstone = new OHashIndexBucket<K, V>(tombstoneCacheEntry, keySerializer, valueSerializer,
              keyTypes, ODurablePage.TrackMode.NONE);
          metadataPage.setTombstoneIndex(newFileLevel, tombstone.getNextRemovedBucketPair());

          updatedBucketIndex = tombstoneIndex;
        } finally {
          diskCache.release(tombstoneCacheEntry);
        }
      } else {
          updatedBucketIndex = diskCache.getFilledUpTo(metadataPage.getFileId(newFileLevel));
      }

      final long newBucketIndex = updatedBucketIndex + 1;

      final OCacheEntry updatedBucketCacheEntry = loadPageEntry(updatedBucketIndex, newFileLevel);
      updatedBucketCacheEntry.acquireExclusiveLock();
      try {
        final OCacheEntry newBucketCacheEntry = loadPageEntry(newBucketIndex, newFileLevel);

        newBucketCacheEntry.acquireExclusiveLock();
        try {
          final OHashIndexBucket<K, V> updatedBucket = new OHashIndexBucket<K, V>(newBucketDepth, updatedBucketCacheEntry,
              keySerializer, valueSerializer, keyTypes, getTrackMode());
          final OHashIndexBucket<K, V> newBucket = new OHashIndexBucket<K, V>(newBucketDepth, newBucketCacheEntry, keySerializer,
              valueSerializer, keyTypes, getTrackMode());

          splitBucketContent(bucket, updatedBucket, newBucket, newBucketDepth);

          assert bucket.getDepth() == bucketDepth;

          metadataPage.setBucketsCount(fileLevel, metadataPage.getBucketsCount(fileLevel) - 1);

          assert metadataPage.getBucketsCount(fileLevel) >= 0;

          updatedBucket.setSplitHistory(fileLevel, pageIndex);
          newBucket.setSplitHistory(fileLevel, pageIndex);

          metadataPage.setBucketsCount(newFileLevel, metadataPage.getBucketsCount(newFileLevel) + 2);

          final long updatedBucketPointer = createBucketPointer(updatedBucketIndex, newFileLevel);
          final long newBucketPointer = createBucketPointer(newBucketIndex, newFileLevel);

          logPageChanges(metadataPage, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);

          logPageChanges(updatedBucket, updatedBucketCacheEntry.getFileId(), updatedBucketCacheEntry.getPageIndex(),
              tombstoneIndex < 0);
          logPageChanges(newBucket, newBucketCacheEntry.getFileId(), newBucketCacheEntry.getPageIndex(), tombstoneIndex < 0);

          return new BucketSplitResult(updatedBucketPointer, newBucketPointer, newBucketDepth);
        } finally {
          newBucketCacheEntry.releaseExclusiveLock();
          newBucketCacheEntry.markDirty();
          diskCache.release(newBucketCacheEntry);
        }
      } finally {
        updatedBucketCacheEntry.releaseExclusiveLock();
        updatedBucketCacheEntry.markDirty();
        diskCache.release(updatedBucketCacheEntry);
      }
    } finally {
      hashStateEntry.releaseExclusiveLock();
      diskCache.release(hashStateEntry);
    }
  }

  private boolean checkBucketDepth(OHashIndexBucket<K, V> bucket) {
    int bucketDepth = bucket.getDepth();

    if (bucket.size() == 0) {
        return true;
    }

    final Iterator<OHashIndexBucket.Entry<K, V>> positionIterator = bucket.iterator();

    long firstValue = keyHashFunction.hashCode(positionIterator.next().key) >>> (HASH_CODE_SIZE - bucketDepth);
    while (positionIterator.hasNext()) {
      final long value = keyHashFunction.hashCode(positionIterator.next().key) >>> (HASH_CODE_SIZE - bucketDepth);
      if (value != firstValue) {
          return false;
      }
    }

    return true;
  }

  private void updateBucket(int nodeIndex, int itemIndex, int offset, long newBucketPointer) throws IOException {
    final long position = directory.getNodePointer(nodeIndex, itemIndex + offset);
    if (position >= 0) {
        directory.setNodePointer(nodeIndex, itemIndex + offset, newBucketPointer);
    } else {
      final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      final int childOffset = (int) (position & 0xFF);
      final int childNodeDepth = directory.getNodeLocalDepth(childNodeIndex);
      final int interval = 1 << childNodeDepth;
      for (int i = 0; i < interval; i++) {
        updateBucket(childNodeIndex, i, childOffset, newBucketPointer);
      }
    }
  }

  private void initHashTreeState() throws IOException {
    for (long pageIndex = 0; pageIndex < MAX_LEVEL_SIZE; pageIndex++) {
      final OCacheEntry cacheEntry = loadPageEntry(pageIndex, 0);
      cacheEntry.acquireExclusiveLock();
      try {
        final OHashIndexBucket<K, V> emptyBucket = new OHashIndexBucket<K, V>(MAX_LEVEL_DEPTH, cacheEntry, keySerializer,
            valueSerializer, keyTypes, getTrackMode());
        cacheEntry.markDirty();

        logPageChanges(emptyBucket, cacheEntry.getFileId(), cacheEntry.getPageIndex(), true);
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }
    }

    final long[] rootTree = new long[MAX_LEVEL_SIZE];
    for (int i = 0; i < MAX_LEVEL_SIZE; i++) {
        rootTree[i] = createBucketPointer(i, 0);
    }

    directory.clear();
    directory.addNewNode((byte) 0, (byte) 0, (byte) MAX_LEVEL_DEPTH, rootTree);

    diskCache.loadPinnedPage(hashStateEntry);
    hashStateEntry.acquireExclusiveLock();
    try {
      OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getTrackMode(), false);
      metadataPage.setBucketsCount(0, MAX_LEVEL_SIZE);
      metadataPage.setRecordsCount(0);

      hashStateEntry.markDirty();
      logPageChanges(metadataPage, hashStateEntry.getFileId(), hashStateEntry.getPageIndex(), false);
    } finally {
      hashStateEntry.releaseExclusiveLock();
      diskCache.release(hashStateEntry);
    }
  }

  private long createBucketPointer(long pageIndex, int fileLevel) {
    return ((pageIndex + 1) << 8) | fileLevel;
  }

  private long getPageIndex(long bucketPointer) {
    return (bucketPointer >>> 8) - 1;
  }

  private int getFileLevel(long bucketPointer) {
    return (int) (bucketPointer & 0xFF);
  }

  private OCacheEntry loadPageEntry(long pageIndex, int fileLevel) throws IOException {
    final long fileId;
    diskCache.loadPinnedPage(hashStateEntry);
    try {
      OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry,
          ODurablePage.TrackMode.NONE, false);
      fileId = metadataPage.getFileId(fileLevel);
    } finally {
      diskCache.release(hashStateEntry);
    }
    return diskCache.load(fileId, pageIndex, false);
  }

  private BucketPath getBucket(final long hashCode) throws IOException {
    int localNodeDepth = directory.getNodeLocalDepth(0);
    int nodeDepth = localNodeDepth;
    BucketPath parentNode = null;
    int nodeIndex = 0;
    int offset = 0;

    int index = (int) ((hashCode >>> (HASH_CODE_SIZE - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));
    BucketPath currentNode = new BucketPath(parentNode, 0, index, 0, localNodeDepth, nodeDepth);
    do {
      final long position = directory.getNodePointer(nodeIndex, index + offset);
      if (position >= 0) {
          return currentNode;
      }

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & 0xFF);

      localNodeDepth = directory.getNodeLocalDepth(nodeIndex);
      nodeDepth += localNodeDepth;

      index = (int) ((hashCode >>> (HASH_CODE_SIZE - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));

      parentNode = currentNode;
      currentNode = new BucketPath(parentNode, offset, index, nodeIndex, localNodeDepth, nodeDepth);
    } while (nodeDepth <= HASH_CODE_SIZE);

    throw new IllegalStateException("Extendible hashing tree in corrupted state.");
  }

  private static final class BucketPath {
    private final BucketPath parent;
    private final int        hashMapOffset;
    private final int        itemIndex;
    private final int        nodeIndex;
    private final int        nodeGlobalDepth;
    private final int        nodeLocalDepth;

    private BucketPath(BucketPath parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeLocalDepth, int nodeGlobalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }

  private static final class BucketSplitResult {
    private final long updatedBucketPointer;
    private final long newBucketPointer;
    private final int  newDepth;

    private BucketSplitResult(long updatedBucketPointer, long newBucketPointer, int newDepth) {
      this.updatedBucketPointer = updatedBucketPointer;
      this.newBucketPointer = newBucketPointer;
      this.newDepth = newDepth;
    }
  }

  private static final class NodeSplitResult {
    private final long[]  newNode;
    private final boolean allLeftHashMapsEqual;
    private final boolean allRightHashMapsEqual;

    private NodeSplitResult(long[] newNode, boolean allLeftHashMapsEqual, boolean allRightHashMapsEqual) {
      this.newNode = newNode;
      this.allLeftHashMapsEqual = allLeftHashMapsEqual;
      this.allRightHashMapsEqual = allRightHashMapsEqual;
    }
  }

  private static final class KeyHashCodeComparator<K> implements Comparator<K> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    private final OHashFunction<K>      keyHashFunction;

    public KeyHashCodeComparator(OHashFunction<K> keyHashFunction) {
      this.keyHashFunction = keyHashFunction;
    }

    @Override
    public int compare(K keyOne, K keyTwo) {
      final long hashCodeOne = keyHashFunction.hashCode(keyOne);
      final long hashCodeTwo = keyHashFunction.hashCode(keyTwo);

      if (greaterThanUnsigned(hashCodeOne, hashCodeTwo)) {
          return 1;
      }
      if (lessThanUnsigned(hashCodeOne, hashCodeTwo)) {
          return -1;
      }

      return comparator.compare(keyOne, keyTwo);
    }

    private static boolean lessThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
    }

    private static boolean greaterThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
    }
  }
}
