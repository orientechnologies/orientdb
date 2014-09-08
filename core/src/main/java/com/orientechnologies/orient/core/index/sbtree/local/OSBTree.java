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

package com.orientechnologies.orient.core.index.sbtree.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * This is implementation which is based on B+-tree implementation threaded tree.
 * 
 * The main differences are:
 * <ol>
 * <li>Buckets are not compacted/removed if they are empty after deletion of item. They reused later when new items are added.</li>
 * <li>All non-leaf buckets have links to neighbor buckets which contain keys which are less/more than keys contained in current
 * bucket</li>
 * <ol/>
 * 
 * 
 * There is support of null values for keys, but values itself can not be null. Null keys support is switched off by default if null
 * keys are supported value which is related to null key will be stored in separate file which has only one page.
 * 
 * Buckets/pages for usual (non-null) key-value entries can be considered as sorted array. The first bytes of page contains such
 * auxiliary information as size of entries contained in bucket, links to neighbors which contain entries with keys less/more than
 * keys in current bucket.
 * 
 * The next bytes contain sorted array of entries. Array itself is split on two parts. First part is growing from start to end, and
 * second part is growing from end to start.
 * 
 * First part is array of offsets to real key-value entries which are stored in second part of array which grows from end to start.
 * This array of offsets is sorted by accessing order according to key value. So we can use binary search to find requested key.
 * When new key-value pair is added we append binary presentation of this pair to the second part of array which grows from end of
 * page to start, remember value of offset for this pair, and find proper position of this offset inside of first part of array.
 * Such approach allows to minimize amount of memory involved in performing of operations and as result speed up data processing.
 * 
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTree<K, V> extends ODurableComponent {
  private static final int                    MAX_KEY_SIZE            = OGlobalConfiguration.SBTREE_MAX_KEY_SIZE
                                                                          .getValueAsInteger();
  private static final int                    MAX_EMBEDDED_VALUE_SIZE = OGlobalConfiguration.SBTREE_MAX_EMBEDDED_VALUE_SIZE
                                                                          .getValueAsInteger();
  private static final OAlwaysLessKey         ALWAYS_LESS_KEY         = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey      ALWAYS_GREATER_KEY      = new OAlwaysGreaterKey();

  private final static long                   ROOT_INDEX              = 0;
  private static final ODurablePage.TrackMode txTrackMode             = ODurablePage.TrackMode
                                                                          .valueOf(OGlobalConfiguration.INDEX_TX_MODE
                                                                              .getValueAsString().toUpperCase());
  private final Comparator<? super K>         comparator              = ODefaultComparator.INSTANCE;
  private final String                        dataFileExtension;
  private final String                        nullFileExtension;
  private final boolean                       durableInNonTxMode;
  private OStorageLocalAbstract               storage;
  private String                              name;
  private ODiskCache                          diskCache;
  private long                                fileId;
  private long                                nullBucketFileId        = -1;
  private int                                 keySize;
  private OBinarySerializer<K>                keySerializer;
  private OType[]                             keyTypes;
  private OBinarySerializer<V>                valueSerializer;
  private boolean                             nullPointerSupport;

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether lowest
   * or highest partially matched key should be used.
   */
  private static enum PartialSearchMode {
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
    private long        pageIndex;
    private int         itemIndex;

    private List<K>     keysCache    = new ArrayList<K>();
    private Iterator<K> keysIterator = new OEmptyIterator<K>();

    public OSBTreeFullKeyCursor(long startPageIndex) {
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

      acquireSharedLock();
      try {
        while (keysCache.size() < prefetchSize) {
          if (pageIndex == -1)
            break;

          if (pageIndex >= diskCache.getFilledUpTo(fileId)) {
            pageIndex = -1;
            break;
          }

          final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
          try {
            final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
                ODurablePage.TrackMode.NONE);

            if (itemIndex >= bucket.size()) {
              pageIndex = bucket.getRightSibling();
              itemIndex = 0;
              continue;
            }

            final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
            itemIndex++;

            keysCache.add(entry.getKey());
          } finally {
            diskCache.release(cacheEntry);
          }
        }
      } catch (IOException e) {
        throw new OSBTreeException("Error during element iteration", e);
      } finally {
        releaseSharedLock();
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
    private final K                   fromKey;
    private final K                   toKey;
    private final boolean             fromKeyInclusive;
    private final boolean             toKeyInclusive;

    private long                      pageIndex;
    private int                       itemIndex;

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
      if (dataCacheIterator == null)
        return null;

      if (dataCacheIterator.hasNext())
        return dataCacheIterator.next();

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger())
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

      if (prefetchSize == 0)
        prefetchSize = 1;

      acquireSharedLock();
      try {
        while (dataCache.size() < prefetchSize) {
          if (pageIndex == -1)
            break;

          if (pageIndex >= diskCache.getFilledUpTo(fileId)) {
            pageIndex = -1;
            break;
          }

          final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
          try {
            final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
                ODurablePage.TrackMode.NONE);

            if (itemIndex >= bucket.size()) {
              pageIndex = bucket.getRightSibling();
              itemIndex = 0;
              continue;
            }

            final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
            itemIndex++;

            if (fromKey != null
                && (fromKeyInclusive ? comparator.compare(entry.getKey(), fromKey) < 0 : comparator
                    .compare(entry.getKey(), fromKey) <= 0))
              continue;

            if (toKey != null
                && (toKeyInclusive ? comparator.compare(entry.getKey(), toKey) > 0 : comparator.compare(entry.getKey(), toKey) >= 0)) {
              pageIndex = -1;
              break;
            }

            dataCache.add(entry);
          } finally {
            diskCache.release(cacheEntry);
          }
        }
      } catch (IOException e) {
        throw new OSBTreeException("Error during element iteration", e);
      } finally {
        releaseSharedLock();
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      return dataCacheIterator.next();
    }
  }

  private final class OSBTreeCursorBackward implements OSBTreeCursor<K, V> {
    private final K                   fromKey;
    private final K                   toKey;
    private final boolean             fromKeyInclusive;
    private final boolean             toKeyInclusive;

    private long                      pageIndex;
    private int                       itemIndex;

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
      if (dataCacheIterator == null)
        return null;

      if (dataCacheIterator.hasNext())
        return dataCacheIterator.next();

      dataCache.clear();

      if (prefetchSize < 0 || prefetchSize > OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger())
        prefetchSize = OGlobalConfiguration.INDEX_CURSOR_PREFETCH_SIZE.getValueAsInteger();

      acquireSharedLock();
      try {
        while (dataCache.size() < prefetchSize) {
          if (pageIndex >= diskCache.getFilledUpTo(fileId))
            pageIndex = diskCache.getFilledUpTo(fileId) - 1;

          if (pageIndex == -1)
            break;

          final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
          try {
            final OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
                ODurablePage.TrackMode.NONE);

            if (itemIndex >= bucket.size())
              itemIndex = bucket.size() - 1;

            if (itemIndex < 0) {
              pageIndex = bucket.getLeftSibling();
              itemIndex = Integer.MAX_VALUE;
              continue;
            }

            final Map.Entry<K, V> entry = convertToMapEntry(bucket.getEntry(itemIndex));
            itemIndex--;

            if (toKey != null
                && (toKeyInclusive ? comparator.compare(entry.getKey(), toKey) > 0 : comparator.compare(entry.getKey(), toKey) >= 0))
              continue;

            if (fromKey != null
                && (fromKeyInclusive ? comparator.compare(entry.getKey(), fromKey) < 0 : comparator
                    .compare(entry.getKey(), fromKey) <= 0)) {
              pageIndex = -1;
              break;
            }

            dataCache.add(entry);
          } finally {
            diskCache.release(cacheEntry);
          }
        }
      } catch (IOException e) {
        throw new OSBTreeException("Error during element iteration", e);
      } finally {
        releaseSharedLock();
      }

      if (dataCache.isEmpty()) {
        dataCacheIterator = null;
        return null;
      }

      dataCacheIterator = dataCache.iterator();

      return dataCacheIterator.next();
    }
  }

  public OSBTree(String dataFileExtension, int keySize, boolean durableInNonTxMode, String nullFileExtension) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    acquireExclusiveLock();
    try {
      this.dataFileExtension = dataFileExtension;
      this.keySize = keySize;
      this.nullFileExtension = nullFileExtension;
      this.durableInNonTxMode = durableInNonTxMode;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      OStorageLocalAbstract storageLocal, boolean nullPointerSupport) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.keyTypes = keyTypes;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.nullPointerSupport = nullPointerSupport;

      fileId = diskCache.openFile(name + dataFileExtension);

      if (nullPointerSupport)
        nullBucketFileId = diskCache.openFile(name + nullFileExtension);

      initDurableComponent(storageLocal);

      OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      rootCacheEntry.acquireExclusiveLock();
      try {
        super.startAtomicOperation();

        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, true, keySerializer, keyTypes, valueSerializer,
            getTrackMode());
        rootBucket.setTreeSize(0);

        super.logPageChanges(rootBucket, fileId, ROOT_INDEX, true);
        rootCacheEntry.markDirty();
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
    } finally {
      releaseExclusiveLock();
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

  public V get(K key) {
    acquireSharedLock();
    try {
      checkNullSupport(key);

      if (key != null) {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        BucketSearchResult bucketSearchResult = findBucket(key);
        if (bucketSearchResult.itemIndex < 0)
          return null;

        long pageIndex = bucketSearchResult.getLastPathItem();
        OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, pageIndex, false);
        try {
          OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer,
              ODurablePage.TrackMode.NONE);

          OSBTreeBucket.SBTreeEntry<K, V> treeEntry = keyBucket.getEntry(bucketSearchResult.itemIndex);
          return readValue(treeEntry.value);
        } finally {
          diskCache.release(keyBucketCacheEntry);
        }
      } else {
        if (diskCache.getFilledUpTo(nullBucketFileId) == 0)
          return null;

        final OCacheEntry nullBucketCacheEntry = diskCache.load(nullBucketFileId, 0, false);
        try {
          final ONullBucket<V> nullBucket = new ONullBucket<V>(nullBucketCacheEntry, ODurablePage.TrackMode.NONE, valueSerializer,
              false);
          final OSBTreeValue<V> treeValue = nullBucket.getValue();
          if (treeValue == null)
            return null;

          return readValue(treeValue);
        } finally {
          diskCache.release(nullBucketCacheEntry);
        }
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving  of sbtree with name " + name, e);
    } finally {
      releaseSharedLock();
    }
  }

  public void put(K key, V value) {
    acquireExclusiveLock();
    final OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      checkNullSupport(key);

      if (key != null) {
        final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);

        final int valueSize = valueSerializer.getObjectSize(value);
        if (keySize > MAX_KEY_SIZE)
          throw new OSBTreeException("Key size is more than allowed, operation was canceled. Current key size " + keySize
              + ", allowed  " + MAX_KEY_SIZE);

        final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        startAtomicOperation();
        try {
          long valueLink = -1;
          if (createLinkToTheValue)
            valueLink = createLinkToTheValue(value);

          final OSBTreeValue<V> treeValue = new OSBTreeValue<V>(createLinkToTheValue, valueLink, createLinkToTheValue ? null
              : value);
          BucketSearchResult bucketSearchResult = findBucket(key);

          OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
          keyBucketCacheEntry.acquireExclusiveLock();
          OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer,
              getTrackMode());

          int insertionIndex;
          int sizeDiff;
          if (bucketSearchResult.itemIndex >= 0) {
            int updateResult = keyBucket.updateValue(bucketSearchResult.itemIndex, treeValue);

            if (updateResult == 1) {
              logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
              keyBucketCacheEntry.markDirty();
            }

            if (updateResult >= 0) {
              keyBucketCacheEntry.releaseExclusiveLock();
              diskCache.release(keyBucketCacheEntry);

              endAtomicOperation(false);
              return;
            } else {
              assert updateResult == -1;

              long removedLinkedValue = keyBucket.remove(bucketSearchResult.itemIndex);
              if (removedLinkedValue >= 0)
                removeLinkedValue(removedLinkedValue);

              insertionIndex = bucketSearchResult.itemIndex;
              sizeDiff = 0;
            }
          } else {
            insertionIndex = -bucketSearchResult.itemIndex - 1;
            sizeDiff = 1;
          }

          while (!keyBucket.addEntry(insertionIndex, new OSBTreeBucket.SBTreeEntry<K, V>(-1, -1, key, treeValue), true)) {
            logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);

            keyBucketCacheEntry.releaseExclusiveLock();
            diskCache.release(keyBucketCacheEntry);

            bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);

            insertionIndex = bucketSearchResult.itemIndex;

            keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
            keyBucketCacheEntry.acquireExclusiveLock();

            keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer, getTrackMode());
          }

          logPageChanges(keyBucket, fileId, bucketSearchResult.getLastPathItem(), false);

          keyBucketCacheEntry.markDirty();
          keyBucketCacheEntry.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          if (sizeDiff != 0)
            setSize(size() + sizeDiff);

          endAtomicOperation(false);
        } catch (Throwable e) {
          rollback(transaction);
          throw new OSBTreeException(e);
        }
      } else {
        OCacheEntry cacheEntry;
        boolean isNew = false;

        if (diskCache.getFilledUpTo(nullBucketFileId) == 0) {
          cacheEntry = diskCache.allocateNewPage(nullBucketFileId);
          isNew = true;
        } else
          cacheEntry = diskCache.load(nullBucketFileId, 0, false);

        startAtomicOperation();
        try {
          final int valueSize = valueSerializer.getObjectSize(value);
          final boolean createLinkToTheValue = valueSize > MAX_EMBEDDED_VALUE_SIZE;

          long valueLink = -1;
          if (createLinkToTheValue)
            valueLink = createLinkToTheValue(value);

          final OSBTreeValue<V> treeValue = new OSBTreeValue<V>(createLinkToTheValue, valueLink, createLinkToTheValue ? null
              : value);

          int sizeDiff = 0;

          cacheEntry.acquireExclusiveLock();
          try {
            final ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getTrackMode(), valueSerializer, isNew);

            if (nullBucket.getValue() != null)
              sizeDiff = -1;

            nullBucket.setValue(treeValue);
            logPageChanges(nullBucket, nullBucketFileId, 0, isNew);

            cacheEntry.markDirty();
          } finally {
            cacheEntry.releaseExclusiveLock();
            diskCache.release(cacheEntry);
          }

          sizeDiff++;

          setSize(size() + sizeDiff);
          endAtomicOperation(false);
        } catch (Throwable e) {
          rollback(transaction);
          throw new OSBTreeException(e);
        }
      }
    } catch (IOException e) {
      rollback(transaction);
      throw new OSBTreeException("Error during index update with key " + key + " and value " + value, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void close(boolean flush) {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId, flush);

      if (nullPointerSupport)
        diskCache.closeFile(nullBucketFileId, flush);
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
    OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startAtomicOperation();

      diskCache.truncateFile(fileId);

      if (nullPointerSupport)
        diskCache.truncateFile(nullBucketFileId);

      OCacheEntry cacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      cacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(cacheEntry, true, keySerializer, keyTypes, valueSerializer,
            getTrackMode());

        rootBucket.setTreeSize(0);

        logPageChanges(rootBucket, fileId, ROOT_INDEX, true);
        cacheEntry.markDirty();
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during clear of sbtree with name " + name, e);
    } catch (Throwable e) {
      rollback(transaction);
      throw new OSBTreeException(e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);

      if (nullPointerSupport)
        diskCache.deleteFile(nullBucketFileId);

    } catch (IOException e) {
      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void deleteWithoutLoad(String name, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      final ODiskCache diskCache = storageLocal.getDiskCache();

      final long fileId = diskCache.openFile(name + dataFileExtension);
      diskCache.deleteFile(fileId);

      final long nullFileId = diskCache.openFile(name + nullFileExtension);
      diskCache.deleteFile(nullFileId);
    } catch (IOException ioe) {
      throw new OSBTreeException("Exception during deletion of sbtree " + name, ioe);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(String name, OBinarySerializer<K> keySerializer, OStreamSerializer valueSerializer, OType[] keyTypes,
      OStorageLocalAbstract storageLocal, boolean nullPointerSupport) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.keyTypes = keyTypes;

      diskCache = storage.getDiskCache();

      this.name = name;
      this.nullPointerSupport = nullPointerSupport;

      fileId = diskCache.openFile(name + dataFileExtension);
      if (nullPointerSupport)
        nullBucketFileId = diskCache.openFile(name + nullFileExtension);

      this.keySerializer = keySerializer;
      this.valueSerializer = (OBinarySerializer<V>) valueSerializer;

      initDurableComponent(storageLocal);
    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer,
            ODurablePage.TrackMode.NONE);
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
    OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      if (key != null) {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        BucketSearchResult bucketSearchResult = findBucket(key);
        if (bucketSearchResult.itemIndex < 0)
          return null;

        OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
        keyBucketCacheEntry.acquireExclusiveLock();
        try {
          startAtomicOperation();

          OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketCacheEntry, keySerializer, keyTypes, valueSerializer,
              getTrackMode());

          final OSBTreeValue<V> removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;
          final V value = readValue(removed);

          long removedValueLink = keyBucket.remove(bucketSearchResult.itemIndex);
          if (removedValueLink >= 0)
            removeLinkedValue(removedValueLink);

          logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
          keyBucketCacheEntry.markDirty();

          setSize(size() - 1);
          endAtomicOperation(false);

          return value;
        } catch (Throwable e) {
          rollback(transaction);
          throw new OSBTreeException(e);
        } finally {
          keyBucketCacheEntry.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);
        }
      } else {
        if (diskCache.getFilledUpTo(nullBucketFileId) == 0)
          return null;

        startAtomicOperation();
        try {
          V removedValue = null;

          OCacheEntry nullCacheEntry = diskCache.load(nullBucketFileId, 0, false);
          nullCacheEntry.acquireExclusiveLock();
          try {
            ONullBucket<V> nullBucket = new ONullBucket<V>(nullCacheEntry, getTrackMode(), valueSerializer, false);
            OSBTreeValue<V> treeValue = nullBucket.getValue();
            if (treeValue == null)
              return null;

            removedValue = readValue(treeValue);
            nullBucket.removeValue();
            logPageChanges(nullBucket, nullBucketFileId, 0, false);
          } finally {
            nullCacheEntry.releaseExclusiveLock();
            diskCache.release(nullCacheEntry);
          }

          if (removedValue != null)
            setSize(size() - 1);

          endAtomicOperation(false);

          return removedValue;
        } catch (Throwable e) {
          rollback(transaction);
          throw new OSBTreeException(e);
        }
      }
    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder) {
    acquireSharedLock();
    try {
      if (!ascSortOrder)
        return iterateEntriesMinorDesc(key, inclusive);

      return iterateEntriesMinorAsc(key, inclusive);
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during iteration of minor values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder) {
    acquireSharedLock();
    try {
      if (ascSortOrder)
        return iterateEntriesMajorAsc(key, inclusive);

      return iterateEntriesMajorDesc(key, inclusive);
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during iteration of major values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public K firstKey() {
    acquireSharedLock();
    try {
      final BucketSearchResult searchResult = firstItem();
      if (searchResult == null)
        return null;

      final OCacheEntry cacheEntry = diskCache.load(fileId, searchResult.getLastPathItem(), false);
      try {
        OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
            ODurablePage.TrackMode.NONE);
        return bucket.getKey(searchResult.itemIndex);
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
      final BucketSearchResult searchResult = lastItem();
      if (searchResult == null)
        return null;

      final OCacheEntry cacheEntry = diskCache.load(fileId, searchResult.getLastPathItem(), false);
      try {
        OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
            ODurablePage.TrackMode.NONE);
        return bucket.getKey(searchResult.itemIndex);
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding last key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  public OSBTreeKeyCursor<K> keyCursor() {
    acquireSharedLock();
    try {
      final BucketSearchResult searchResult = firstItem();
      if (searchResult == null)
        return new OSBTreeKeyCursor<K>() {
          @Override
          public K next(int prefetchSize) {
            return null;
          }
        };

      return new OSBTreeFullKeyCursor(searchResult.getLastPathItem());
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  public OSBTreeCursor<K, V> iterateEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      boolean ascSortOrder) {
    acquireSharedLock();
    try {
      if (ascSortOrder)
        return iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive);
      else
        return iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive);

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

    final ODurablePage.TrackMode trackMode = super.getTrackMode();
    if (!trackMode.equals(ODurablePage.TrackMode.NONE))
      return txTrackMode;

    return trackMode;
  }

  private void initDurableComponent(OStorageLocalAbstract storageLocal) {
    init(storageLocal);
  }

  private void checkNullSupport(K key) {
    if (key == null && !nullPointerSupport)
      throw new OSBTreeException("Null keys are not supported.");
  }

  private void removeLinkedValue(long removedLink) throws IOException {
    long nextPage = removedLink;
    do {
      removedLink = nextPage;

      OCacheEntry valueEntry = diskCache.load(fileId, removedLink, false);
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(valueEntry, getTrackMode(), false);
        nextPage = valuePage.getNextPage();
      } finally {
        diskCache.release(valueEntry);
      }

      removeValuePage(removedLink);
    } while (nextPage >= 0);
  }

  private void removeValuePage(long pageIndex) throws IOException {
    long prevFreeListItem;
    OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
    rootCacheEntry.acquireExclusiveLock();
    OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer,
        getTrackMode());
    try {
      prevFreeListItem = rootBucket.getValuesFreeListFirstIndex();
      rootBucket.setValuesFreeListFirstIndex(pageIndex);

      rootCacheEntry.markDirty();
      logPageChanges(rootBucket, fileId, ROOT_INDEX, false);
    } finally {
      rootCacheEntry.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }

    OCacheEntry valueEntry = diskCache.load(fileId, pageIndex, false);
    valueEntry.acquireExclusiveLock();
    try {
      OSBTreeValuePage valuePage = new OSBTreeValuePage(valueEntry, getTrackMode(), false);
      valuePage.setNextFreeListPage(prevFreeListItem);

      valueEntry.markDirty();
      logPageChanges(valuePage, fileId, pageIndex, false);
    } finally {
      valueEntry.releaseExclusiveLock();
      diskCache.release(valueEntry);
    }
  }

  private long createLinkToTheValue(V value) throws IOException {
    byte[] serializeValue = new byte[valueSerializer.getObjectSize(value)];
    valueSerializer.serializeNative(value, serializeValue, 0);

    final int amountOfPages = OSBTreeValuePage.calculateAmountOfPage(serializeValue.length);

    int position = 0;
    long freeListPageIndex = allocateValuePageFromFreeList();

    OCacheEntry cacheEntry;
    if (freeListPageIndex < 0)
      cacheEntry = diskCache.allocateNewPage(fileId);
    else
      cacheEntry = diskCache.load(fileId, freeListPageIndex, false);

    final long valueLink = cacheEntry.getPageIndex();
    cacheEntry.acquireExclusiveLock();
    try {
      OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, getTrackMode(), freeListPageIndex >= 0);
      position = valuePage.fillBinaryContent(serializeValue, position);

      valuePage.setNextFreeListPage(-1);
      valuePage.setNextPage(-1);

      cacheEntry.markDirty();

      if (freeListPageIndex < 0)
        logPageChanges(valuePage, fileId, cacheEntry.getPageIndex(), true);
      else
        logPageChanges(valuePage, fileId, cacheEntry.getPageIndex(), false);
    } finally {
      cacheEntry.releaseExclusiveLock();
      diskCache.release(cacheEntry);
    }

    long prevPage = valueLink;
    for (int i = 1; i < amountOfPages; i++) {
      freeListPageIndex = allocateValuePageFromFreeList();

      if (freeListPageIndex < 0)
        cacheEntry = diskCache.allocateNewPage(fileId);
      else
        cacheEntry = diskCache.load(fileId, freeListPageIndex, false);

      cacheEntry.acquireExclusiveLock();
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, getTrackMode(), freeListPageIndex >= 0);
        position = valuePage.fillBinaryContent(serializeValue, position);

        valuePage.setNextFreeListPage(-1);
        valuePage.setNextPage(-1);

        cacheEntry.markDirty();
        if (freeListPageIndex < 0)
          logPageChanges(valuePage, fileId, cacheEntry.getPageIndex(), true);
        else
          logPageChanges(valuePage, fileId, cacheEntry.getPageIndex(), false);
      } finally {
        cacheEntry.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      OCacheEntry prevPageCacheEntry = diskCache.load(fileId, prevPage, false);
      prevPageCacheEntry.acquireExclusiveLock();
      try {
        OSBTreeValuePage valuePage = new OSBTreeValuePage(prevPageCacheEntry, getTrackMode(), freeListPageIndex >= 0);
        valuePage.setNextPage(cacheEntry.getPageIndex());

        prevPageCacheEntry.markDirty();
        logPageChanges(valuePage, fileId, prevPage, false);
      } finally {
        prevPageCacheEntry.releaseExclusiveLock();
        diskCache.release(prevPageCacheEntry);
      }

      prevPage = cacheEntry.getPageIndex();
    }

    return valueLink;
  }

  private long allocateValuePageFromFreeList() throws IOException {
    OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
    OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer,
        ODurablePage.TrackMode.NONE);
    long freeListFirstIndex;
    try {
      freeListFirstIndex = rootBucket.getValuesFreeListFirstIndex();
    } finally {
      diskCache.release(rootCacheEntry);
    }

    if (freeListFirstIndex >= 0) {
      OCacheEntry freePageEntry = diskCache.load(fileId, freeListFirstIndex, false);
      OSBTreeValuePage valuePage = new OSBTreeValuePage(freePageEntry, getTrackMode(), false);
      freePageEntry.acquireExclusiveLock();

      try {
        long nextFreeListIndex = valuePage.getNextFreeListPage();

        rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
        rootCacheEntry.acquireExclusiveLock();
        rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer, getTrackMode());
        try {
          rootBucket.setValuesFreeListFirstIndex(nextFreeListIndex);

          rootCacheEntry.markDirty();
          logPageChanges(rootBucket, fileId, ROOT_INDEX, false);
        } finally {
          rootCacheEntry.releaseExclusiveLock();
          diskCache.release(rootCacheEntry);
        }

        valuePage.setNextFreeListPage(-1);

        freePageEntry.markDirty();
        logPageChanges(valuePage, fileId, freePageEntry.getPageIndex(), false);
      } finally {
        freePageEntry.releaseExclusiveLock();
        diskCache.release(freePageEntry);
      }

      return freePageEntry.getPageIndex();
    }

    return -1;
  }

  private void rollback(OStorageTransaction transaction) {
    try {
      endAtomicOperation(true);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  private void setSize(long size) throws IOException {
    OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
    rootCacheEntry.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootCacheEntry, keySerializer, keyTypes, valueSerializer,
          getTrackMode());
      rootBucket.setTreeSize(size);

      logPageChanges(rootBucket, fileId, ROOT_INDEX, false);
      rootCacheEntry.markDirty();
    } finally {
      rootCacheEntry.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorDesc(K key, boolean inclusive) throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorDesc(key, inclusive);

    BucketSearchResult bucketSearchResult = findBucket(key);

    long pageIndex = bucketSearchResult.getLastPathItem();
    int index;
    if (bucketSearchResult.itemIndex >= 0) {
      index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
    } else {
      index = -bucketSearchResult.itemIndex - 2;
    }

    return new OSBTreeCursorBackward(pageIndex, index, null, key, false, inclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesMinorAsc(K key, boolean inclusive) throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMinorAsc(key, inclusive);

    final BucketSearchResult searchResult;
    acquireSharedLock();
    try {
      searchResult = firstItem();
      if (searchResult == null)
        return new OSBTreeCursor<K, V>() {
          @Override
          public Map.Entry<K, V> next(int prefetchSize) {
            return null;
          }
        };

    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }

    return new OSBTreeCursorForward(searchResult.getLastPathItem(), searchResult.itemIndex, null, key, false, inclusive);
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

  private OSBTreeCursor<K, V> iterateEntriesMajorAsc(K key, boolean inclusive) throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorAsc(key, inclusive);

    BucketSearchResult bucketSearchResult = findBucket(key);

    long pageIndex = bucketSearchResult.getLastPathItem();
    int index;
    if (bucketSearchResult.itemIndex >= 0) {
      index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
    } else {
      index = -bucketSearchResult.itemIndex - 1;
    }

    return new OSBTreeCursorForward(pageIndex, index, key, null, inclusive, false);
  }

  private OSBTreeCursor<K, V> iterateEntriesMajorDesc(K key, boolean inclusive) throws IOException {
    key = keySerializer.preprocess(key, (Object[]) keyTypes);
    key = enhanceCompositeKeyMajorDesc(key, inclusive);

    final BucketSearchResult searchResult;
    acquireSharedLock();
    try {
      searchResult = lastItem();
      if (searchResult == null)
        return new OSBTreeCursor<K, V>() {
          @Override
          public Map.Entry<K, V> next(int prefetchSize) {
            return null;
          }
        };

    } catch (IOException e) {
      throw new OSBTreeException("Error during finding last key in sbtree [" + name + "]");
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

  private BucketSearchResult firstItem() throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = diskCache.load(fileId, bucketIndex, false);
    int itemIndex = 0;

    OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
        ODurablePage.TrackMode.NONE);
    try {
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

        diskCache.release(cacheEntry);
        cacheEntry = diskCache.load(fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer, ODurablePage.TrackMode.NONE);
      }
    } finally {
      diskCache.release(cacheEntry);
    }
  }

  private BucketSearchResult lastItem() throws IOException {
    LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = diskCache.load(fileId, bucketIndex, false);
    OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer,
        ODurablePage.TrackMode.NONE);

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

        diskCache.release(cacheEntry);
        cacheEntry = diskCache.load(fileId, bucketIndex, false);

        bucket = new OSBTreeBucket<K, V>(cacheEntry, keySerializer, keyTypes, valueSerializer, ODurablePage.TrackMode.NONE);
        if (itemIndex == OSBTreeBucket.MAX_PAGE_SIZE_BYTES + 1)
          itemIndex = bucket.size() - 1;
      }
    } finally {
      diskCache.release(cacheEntry);
    }
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenAscOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive)
      throws IOException {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenAsc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenAsc(keyTo, toInclusive);

    BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom);

    long pageIndexFrom = bucketSearchResultFrom.getLastPathItem();

    int indexFrom;
    if (bucketSearchResultFrom.itemIndex >= 0) {
      indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
    } else {
      indexFrom = -bucketSearchResultFrom.itemIndex - 1;
    }

    return new OSBTreeCursorForward(pageIndexFrom, indexFrom, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private OSBTreeCursor<K, V> iterateEntriesBetweenDescOrder(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive)
      throws IOException {
    keyFrom = keySerializer.preprocess(keyFrom, (Object[]) keyTypes);
    keyTo = keySerializer.preprocess(keyTo, (Object[]) keyTypes);

    keyFrom = enhanceFromCompositeKeyBetweenDesc(keyFrom, fromInclusive);
    keyTo = enhanceToCompositeKeyBetweenDesc(keyTo, toInclusive);

    BucketSearchResult bucketSearchResultTo = findBucket(keyTo);

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

  private BucketSearchResult splitBucket(List<Long> path, int keyIndex, K keyToInsert) throws IOException {
    long pageIndex = path.get(path.size() - 1);
    OCacheEntry bucketEntry = diskCache.load(fileId, pageIndex, false);

    bucketEntry.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> bucketToSplit = new OSBTreeBucket<K, V>(bucketEntry, keySerializer, keyTypes, valueSerializer,
          getTrackMode());

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
            rightEntries);
      } else {
        return splitRootBucket(path, keyIndex, keyToInsert, pageIndex, bucketEntry, bucketToSplit, splitLeaf, indexToSplit,
            separationKey, rightEntries);
      }
    } finally {
      bucketEntry.releaseExclusiveLock();
      diskCache.release(bucketEntry);
    }
  }

  private BucketSearchResult splitNonRootBucket(List<Long> path, int keyIndex, K keyToInsert, long pageIndex,
      OSBTreeBucket<K, V> bucketToSplit, boolean splitLeaf, int indexToSplit, K separationKey,
      List<OSBTreeBucket.SBTreeEntry<K, V>> rightEntries) throws IOException {
    OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
    rightBucketEntry.acquireExclusiveLock();

    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer, getTrackMode());
      newRightBucket.addAll(rightEntries);

      bucketToSplit.shrink(indexToSplit);

      if (splitLeaf) {
        long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry = diskCache.load(fileId, rightSiblingPageIndex, false);
          rightSiblingBucketEntry.acquireExclusiveLock();
          OSBTreeBucket<K, V> rightSiblingBucket = new OSBTreeBucket<K, V>(rightSiblingBucketEntry, keySerializer, keyTypes,
              valueSerializer, getTrackMode());
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
            logPageChanges(rightSiblingBucket, fileId, rightSiblingPageIndex, false);

            rightSiblingBucketEntry.markDirty();
          } finally {
            rightSiblingBucketEntry.releaseExclusiveLock();
            diskCache.release(rightSiblingBucketEntry);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = diskCache.load(fileId, parentIndex, false);
      parentCacheEntry.acquireExclusiveLock();
      try {
        OSBTreeBucket<K, V> parentBucket = new OSBTreeBucket<K, V>(parentCacheEntry, keySerializer, keyTypes, valueSerializer,
            getTrackMode());
        OSBTreeBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBucket.SBTreeEntry<K, V>(pageIndex,
            rightBucketEntry.getPageIndex(), separationKey, null);

        int insertionIndex = parentBucket.find(separationKey);
        assert insertionIndex < 0;

        insertionIndex = -insertionIndex - 1;
        while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
          parentCacheEntry.releaseExclusiveLock();
          diskCache.release(parentCacheEntry);

          BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

          parentIndex = bucketSearchResult.getLastPathItem();
          parentCacheEntry = diskCache.load(fileId, parentIndex, false);
          parentCacheEntry.acquireExclusiveLock();

          insertionIndex = bucketSearchResult.itemIndex;

          parentBucket = new OSBTreeBucket<K, V>(parentCacheEntry, keySerializer, keyTypes, valueSerializer, getTrackMode());
        }

        logPageChanges(parentBucket, fileId, parentIndex, false);
      } finally {
        parentCacheEntry.releaseExclusiveLock();
        diskCache.release(parentCacheEntry);
      }

      logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
    } finally {
      rightBucketEntry.markDirty();
      rightBucketEntry.releaseExclusiveLock();
      diskCache.release(rightBucketEntry);
    }

    logPageChanges(bucketToSplit, fileId, pageIndex, false);
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
      List<OSBTreeBucket.SBTreeEntry<K, V>> rightEntries) throws IOException {
    final long freeListPage = bucketToSplit.getValuesFreeListFirstIndex();
    final long treeSize = bucketToSplit.getTreeSize();

    final List<OSBTreeBucket.SBTreeEntry<K, V>> leftEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K, V>>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++)
      leftEntries.add(bucketToSplit.getEntry(i));

    OCacheEntry leftBucketEntry = diskCache.allocateNewPage(fileId);

    OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
    leftBucketEntry.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> newLeftBucket = new OSBTreeBucket<K, V>(leftBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer, getTrackMode());
      newLeftBucket.addAll(leftEntries);

      if (splitLeaf)
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());

      logPageChanges(newLeftBucket, fileId, leftBucketEntry.getPageIndex(), true);
    } finally {
      leftBucketEntry.releaseExclusiveLock();
      diskCache.release(leftBucketEntry);
    }

    rightBucketEntry.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketEntry, splitLeaf, keySerializer, keyTypes,
          valueSerializer, getTrackMode());
      newRightBucket.addAll(rightEntries);

      if (splitLeaf)
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());

      logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
      rightBucketEntry.markDirty();
    } finally {
      rightBucketEntry.releaseExclusiveLock();
      diskCache.release(rightBucketEntry);
    }

    bucketToSplit = new OSBTreeBucket<K, V>(bucketEntry, false, keySerializer, keyTypes, valueSerializer, getTrackMode());

    bucketToSplit.setTreeSize(treeSize);
    bucketToSplit.setValuesFreeListFirstIndex(freeListPage);

    bucketToSplit.addEntry(0, new OSBTreeBucket.SBTreeEntry<K, V>(leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(),
        separationKey, null), true);

    logPageChanges(bucketToSplit, fileId, pageIndex, false);
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

  private BucketSearchResult findBucket(K key) throws IOException {
    long pageIndex = ROOT_INDEX;
    final ArrayList<Long> path = new ArrayList<Long>();

    int hop = 0;

    while (true) {
      path.add(pageIndex);
      final OCacheEntry bucketEntry = diskCache.load(fileId, pageIndex, false);
      final OSBTreeBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(bucketEntry, keySerializer, keyTypes, valueSerializer,
            ODurablePage.TrackMode.NONE);
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
        pageIndex = entry.rightChild;
      else
        pageIndex = entry.leftChild;

      if (hop++ > 1000) {
        OLogManager
            .instance()
            .error(
                this,
                "Index '%s' (engine=OSBTree) reached maximum hop=%d on lookup. Index could be corrupted, due to a previous hard kill. Please drop and recreate it",
                getName(), hop);
        return null;
      }
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

  private V readValue(OSBTreeValue<V> sbTreeValue) throws IOException {
    if (!sbTreeValue.isLink())
      return sbTreeValue.getValue();

    OCacheEntry cacheEntry = diskCache.load(fileId, sbTreeValue.getLink(), false);
    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, ODurablePage.TrackMode.NONE, false);

    int totalSize = valuePage.getSize();
    int currentSize = 0;
    byte[] value = new byte[totalSize];

    while (currentSize < totalSize) {
      currentSize = valuePage.readBinaryContent(value, currentSize);

      long nextPage = valuePage.getNextPage();
      if (nextPage >= 0) {
        diskCache.release(cacheEntry);
        cacheEntry = diskCache.load(fileId, nextPage, false);

        valuePage = new OSBTreeValuePage(cacheEntry, ODurablePage.TrackMode.NONE, false);
      }
    }

    diskCache.release(cacheEntry);

    return valueSerializer.deserializeNative(value, 0);
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

}
