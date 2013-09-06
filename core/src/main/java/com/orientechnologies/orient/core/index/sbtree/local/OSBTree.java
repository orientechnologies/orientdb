package com.orientechnologies.orient.core.index.sbtree.local;

import java.io.IOException;
import java.util.*;

import com.orientechnologies.common.collection.OAlwaysGreaterKey;
import com.orientechnologies.common.collection.OAlwaysLessKey;
import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OSBTree<K, V> extends ODurableComponent {
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private final static long              ROOT_INDEX         = 0;

  private final Comparator<? super K>    comparator         = ODefaultComparator.INSTANCE;

  private OStorageLocalAbstract          storage;
  private String                         name;

  private final String                   dataFileExtension;

  private ODiskCache                     diskCache;

  private long                           fileId;

  private int                            keySize;

  private OBinarySerializer<K>           keySerializer;
  private OBinarySerializer<V>           valueSerializer;

  private final boolean                  durableInNonTxMode;

  public OSBTree(String dataFileExtension, int keySize, boolean durableInNonTxMode) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
    this.keySize = keySize;
    this.durableInNonTxMode = durableInNonTxMode;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      fileId = diskCache.openFile(name + dataFileExtension);

      initDurableComponent(storageLocal);

      OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      rootPointer.acquireExclusiveLock();
      try {
        super.startDurableOperation(null);

        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootPointer.getDataPointer(), true, keySerializer,
            valueSerializer, getTrackMode());
        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setValueSerializerId(valueSerializer.getId());
        rootBucket.setTreeSize(0);

        super.logPageChanges(rootBucket, fileId, ROOT_INDEX, true);
        rootCacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(rootCacheEntry);
      }

      super.endDurableOperation(null, false);
    } catch (IOException e) {
      try {
        super.endDurableOperation(null, true);
      } catch (IOException e1) {
        OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
      }
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void initDurableComponent(OStorageLocalAbstract storageLocal) {
    OWriteAheadLog writeAheadLog = storageLocal.getWALInstance();
    init(writeAheadLog);
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
      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      long pageIndex = bucketSearchResult.getLastPathItem();
      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, pageIndex, false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();
      try {
        OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer.getDataPointer(), keySerializer, valueSerializer,
            ODurablePage.TrackMode.NONE);
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

  public void put(K key, V value) {
    acquireExclusiveLock();
    final OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startDurableOperation(transaction);

      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      keyBucketPointer.acquireExclusiveLock();
      OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer.getDataPointer(), keySerializer, valueSerializer,
          getTrackMode());

      if (bucketSearchResult.itemIndex >= 0) {
        while (!keyBucket.updateValue(bucketSearchResult.itemIndex, value)) {
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, bucketSearchResult.itemIndex, key);

          keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
          keyBucketPointer = keyBucketCacheEntry.getCachePointer();
          keyBucketPointer.acquireExclusiveLock();

          keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer.getDataPointer(), keySerializer, valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketSearchResult.getLastPathItem(), false);
      } else {
        int insertionIndex = -bucketSearchResult.itemIndex - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBucket.SBTreeEntry<K, V>(-1, -1, key, value), true)) {
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);

          insertionIndex = bucketSearchResult.itemIndex;

          keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
          keyBucketPointer = keyBucketCacheEntry.getCachePointer();
          keyBucketPointer.acquireExclusiveLock();

          keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer.getDataPointer(), keySerializer, valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketSearchResult.getLastPathItem(), false);

        setSize(size() + 1);
      }

      keyBucketCacheEntry.markDirty();
      keyBucketPointer.releaseExclusiveLock();
      diskCache.release(keyBucketCacheEntry);

      endDurableOperation(transaction, false);
    } catch (IOException e) {
      rollback(transaction);
      throw new OSBTreeException("Error during index update with key " + key + " and value " + value, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void rollback(OStorageTransaction transaction) {
    try {
      endDurableOperation(transaction, true);
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
    OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startDurableOperation(transaction);

      diskCache.truncateFile(fileId);

      OCacheEntry cacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      OCachePointer rootPointer = cacheEntry.getCachePointer();
      rootPointer.acquireExclusiveLock();
      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootPointer.getDataPointer(), true, keySerializer,
            valueSerializer, getTrackMode());

        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setValueSerializerId(valueSerializer.getId());
        rootBucket.setTreeSize(0);

        logPageChanges(rootBucket, fileId, ROOT_INDEX, true);
        cacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      endDurableOperation(transaction, false);
    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during clear of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } catch (IOException e) {
      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(String name, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      diskCache = storage.getDiskCache();

      this.name = name;

      fileId = diskCache.openFile(name + dataFileExtension);

      OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();
      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootPointer.getDataPointer(), keySerializer, valueSerializer,
            ODurablePage.TrackMode.NONE);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getValueSerializerId());
      } finally {
        diskCache.release(rootCacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void setSize(long size) throws IOException {
    OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);

    OCachePointer rootPointer = rootCacheEntry.getCachePointer();
    rootPointer.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootPointer.getDataPointer(), keySerializer, valueSerializer,
          getTrackMode());
      rootBucket.setTreeSize(size);

      logPageChanges(rootBucket, fileId, ROOT_INDEX, false);
      rootCacheEntry.markDirty();
    } finally {
      rootPointer.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      OCacheEntry rootCacheEntry = diskCache.load(fileId, ROOT_INDEX, false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      try {
        OSBTreeBucket<K, V> rootBucket = new OSBTreeBucket<K, V>(rootPointer.getDataPointer(), keySerializer, valueSerializer,
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
      startDurableOperation(transaction);

      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      keyBucketPointer.acquireExclusiveLock();
      try {
        OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer.getDataPointer(), keySerializer, valueSerializer,
            getTrackMode());

        final V removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

        keyBucket.remove(bucketSearchResult.itemIndex);

        logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
        keyBucketCacheEntry.markDirty();

        setSize(size() - 1);
        endDurableOperation(transaction, false);
        return removed;
      } finally {
        keyBucketPointer.releaseExclusiveLock();
        diskCache.release(keyBucketCacheEntry);
      }

    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected void endDurableOperation(OStorageTransaction transaction, boolean rollback) throws IOException {
    if (transaction == null && !durableInNonTxMode)
      return;

    super.endDurableOperation(transaction, rollback);
  }

  @Override
  protected void startDurableOperation(OStorageTransaction transaction) throws IOException {
    if (transaction == null && !durableInNonTxMode)
      return;

    super.startDurableOperation(transaction);
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
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
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
      final PartialSearchMode partialSearchMode;
      if (inclusive)
        partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
      else
        partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;

      BucketSearchResult bucketSearchResult = findBucket(key, partialSearchMode);

      long pageIndex = bucketSearchResult.getLastPathItem();
      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
      } else {
        index = -bucketSearchResult.itemIndex - 2;
      }

      boolean firstBucket = true;
      resultsLoop: while (true) {
        long nextPageIndex = -1;
        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(pointer.getDataPointer(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
          if (!firstBucket)
            index = bucket.size() - 1;

          for (int i = index; i >= 0; i--) {
            if (!listener.addResult(bucket.getEntry(i)))
              break resultsLoop;
          }

          if (bucket.getLeftSibling() >= 0)
            nextPageIndex = bucket.getLeftSibling();
          else
            break;

        } finally {
          diskCache.release(cacheEntry);
        }

        pageIndex = nextPageIndex;
        firstBucket = false;
      }
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
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      final PartialSearchMode partialSearchMode;
      if (inclusive)
        partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
      else
        partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;

      BucketSearchResult bucketSearchResult = findBucket(key, partialSearchMode);
      long pageIndex = bucketSearchResult.getLastPathItem();
      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
      } else {
        index = -bucketSearchResult.itemIndex - 1;
      }

      resultsLoop: while (true) {
        long nextPageIndex = -1;
        final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(pointer.getDataPointer(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
          int bucketSize = bucket.size();
          for (int i = index; i < bucketSize; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              break resultsLoop;
          }

          if (bucket.getRightSibling() >= 0)
            nextPageIndex = bucket.getRightSibling();
          else
            break;
        } finally {
          diskCache.release(cacheEntry);
        }

        pageIndex = nextPageIndex;
        index = 0;
      }

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
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
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

      long bucketIndex = ROOT_INDEX;

      OCacheEntry cacheEntry = diskCache.load(fileId, bucketIndex, false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      int itemIndex = 0;

      OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cachePointer.getDataPointer(), keySerializer, valueSerializer,
          ODurablePage.TrackMode.NONE);
      try {
        while (true) {
          if (!bucket.isLeaf()) {
            if (bucket.isEmpty() || itemIndex >= bucket.size()) {
              if (!path.isEmpty()) {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketIndex = pagePathItemUnit.pageIndex;
                itemIndex = pagePathItemUnit.itemIndex + 1;
              } else
                return null;
            } else {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);

              path.add(new PagePathItemUnit(bucketIndex, itemIndex));

              bucketIndex = entry.leftChild;
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
              return bucket.getKey(0);
            }
          }

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketIndex, false);
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBucket<K, V>(cachePointer.getDataPointer(), keySerializer, valueSerializer,
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

  public K lastKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      long bucketIndex = ROOT_INDEX;

      OCacheEntry cacheEntry = diskCache.load(fileId, bucketIndex, false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(cachePointer.getDataPointer(), keySerializer, valueSerializer,
          ODurablePage.TrackMode.NONE);

      int itemIndex = bucket.size() - 1;
      try {
        while (true) {
          if (!bucket.isLeaf()) {
            if (itemIndex < 0) {
              if (!path.isEmpty()) {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketIndex = pagePathItemUnit.pageIndex;
                itemIndex = pagePathItemUnit.itemIndex - 1;
              } else
                return null;
            } else {
              OSBTreeBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);

              path.add(new PagePathItemUnit(bucketIndex, itemIndex));

              bucketIndex = entry.rightChild;
              itemIndex = OSBTreeBucket.MAX_BUCKET_SIZE_BYTES + 1;
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
              return bucket.getKey(bucket.size() - 1);
            }
          }

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketIndex, false);
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBucket<K, V>(cachePointer.getDataPointer(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
          if (itemIndex == OSBTreeBucket.MAX_BUCKET_SIZE_BYTES + 1)
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
      PartialSearchMode partialSearchModeFrom;
      if (fromInclusive)
        partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
      else
        partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

      BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom, partialSearchModeFrom);

      long pageIndexFrom = bucketSearchResultFrom.getLastPathItem();

      int indexFrom;
      if (bucketSearchResultFrom.itemIndex >= 0) {
        indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
      } else {
        indexFrom = -bucketSearchResultFrom.itemIndex - 1;
      }

      PartialSearchMode partialSearchModeTo;
      if (toInclusive)
        partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
      else
        partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

      BucketSearchResult bucketSearchResultTo = findBucket(keyTo, partialSearchModeTo);
      long pageIndexTo = bucketSearchResultTo.getLastPathItem();

      int indexTo;
      if (bucketSearchResultTo.itemIndex >= 0) {
        indexTo = toInclusive ? bucketSearchResultTo.itemIndex : bucketSearchResultTo.itemIndex - 1;
      } else {
        indexTo = -bucketSearchResultTo.itemIndex - 2;
      }

      int startIndex = indexFrom;
      int endIndex;
      long pageIndex = pageIndexFrom;

      resultsLoop: while (true) {
        long nextPageIndex = -1;

        final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBucket<K, V> bucket = new OSBTreeBucket<K, V>(pointer.getDataPointer(), keySerializer, valueSerializer,
              ODurablePage.TrackMode.NONE);
          if (pageIndex != pageIndexTo)
            endIndex = bucket.size() - 1;
          else
            endIndex = indexTo;

          for (int i = startIndex; i <= endIndex; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              break resultsLoop;
          }

          if (pageIndex == pageIndexTo)
            break;

          if (bucket.getRightSibling() >= 0)
            nextPageIndex = bucket.getRightSibling();
          else
            break;

        } finally {
          diskCache.release(cacheEntry);
        }

        pageIndex = nextPageIndex;
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

  private BucketSearchResult splitBucket(List<Long> path, int keyIndex, K keyToInsert) throws IOException {
    long pageIndex = path.get(path.size() - 1);
    OCacheEntry bucketEntry = diskCache.load(fileId, pageIndex, false);
    OCachePointer bucketPointer = bucketEntry.getCachePointer();

    bucketPointer.acquireExclusiveLock();
    try {
      OSBTreeBucket<K, V> bucketToSplit = new OSBTreeBucket<K, V>(bucketPointer.getDataPointer(), keySerializer, valueSerializer,
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
        OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
        OCachePointer rightBucketPointer = rightBucketEntry.getCachePointer();

        rightBucketPointer.acquireExclusiveLock();

        try {
          OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketPointer.getDataPointer(), splitLeaf,
              keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            long rightSiblingPageIndex = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingPageIndex);
            newRightBucket.setLeftSibling(pageIndex);

            bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

            if (rightSiblingPageIndex >= 0) {
              final OCacheEntry rightSiblingBucketEntry = diskCache.load(fileId, rightSiblingPageIndex, false);
              final OCachePointer rightSiblingPointer = rightSiblingBucketEntry.getCachePointer();

              rightSiblingPointer.acquireExclusiveLock();
              OSBTreeBucket<K, V> rightSiblingBucket = new OSBTreeBucket<K, V>(rightSiblingPointer.getDataPointer(), keySerializer,
                  valueSerializer, getTrackMode());
              try {
                rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
                logPageChanges(rightSiblingBucket, fileId, rightSiblingPageIndex, false);

                rightSiblingBucketEntry.markDirty();
              } finally {
                rightSiblingPointer.releaseExclusiveLock();
                diskCache.release(rightSiblingBucketEntry);
              }
            }
          }

          long parentIndex = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = diskCache.load(fileId, parentIndex, false);
          OCachePointer parentPointer = parentCacheEntry.getCachePointer();

          parentPointer.acquireExclusiveLock();
          try {
            OSBTreeBucket<K, V> parentBucket = new OSBTreeBucket<K, V>(parentPointer.getDataPointer(), keySerializer,
                valueSerializer, getTrackMode());
            OSBTreeBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBucket.SBTreeEntry<K, V>(pageIndex,
                rightBucketEntry.getPageIndex(), separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentPointer.releaseExclusiveLock();
              diskCache.release(parentCacheEntry);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

              parentIndex = bucketSearchResult.getLastPathItem();
              parentCacheEntry = diskCache.load(fileId, parentIndex, false);
              parentPointer = parentCacheEntry.getCachePointer();

              parentPointer.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBucket<K, V>(parentPointer.getDataPointer(), keySerializer, valueSerializer, getTrackMode());
            }

            logPageChanges(parentBucket, fileId, parentIndex, false);
          } finally {
            parentCacheEntry.markDirty();
            parentPointer.releaseExclusiveLock();

            diskCache.release(parentCacheEntry);
          }

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
        } finally {
          rightBucketEntry.markDirty();
          rightBucketPointer.releaseExclusiveLock();
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

      } else {
        long treeSize = bucketToSplit.getTreeSize();

        final List<OSBTreeBucket.SBTreeEntry<K, V>> leftEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K, V>>(indexToSplit);

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        OCacheEntry leftBucketEntry = diskCache.allocateNewPage(fileId);
        OCachePointer leftBucketPointer = leftBucketEntry.getCachePointer();

        OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
        leftBucketPointer.acquireExclusiveLock();
        try {
          OSBTreeBucket<K, V> newLeftBucket = new OSBTreeBucket<K, V>(leftBucketPointer.getDataPointer(), splitLeaf, keySerializer,
              valueSerializer, getTrackMode());
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());

          logPageChanges(newLeftBucket, fileId, leftBucketEntry.getPageIndex(), true);
          leftBucketEntry.markDirty();
        } finally {
          leftBucketPointer.releaseExclusiveLock();
          diskCache.release(leftBucketEntry);
        }

        OCachePointer rightBucketPointer = rightBucketEntry.getCachePointer();
        rightBucketPointer.acquireExclusiveLock();
        try {
          OSBTreeBucket<K, V> newRightBucket = new OSBTreeBucket<K, V>(rightBucketPointer.getDataPointer(), splitLeaf,
              keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
          rightBucketEntry.markDirty();
        } finally {
          rightBucketPointer.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        bucketToSplit = new OSBTreeBucket<K, V>(bucketPointer.getDataPointer(), false, keySerializer, valueSerializer,
            getTrackMode());
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit.addEntry(0,
            new OSBTreeBucket.SBTreeEntry<K, V>(leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), separationKey,
                null), true);

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

    } finally {
      bucketEntry.markDirty();
      bucketPointer.releaseExclusiveLock();
      diskCache.release(bucketEntry);
    }
  }

  private BucketSearchResult findBucket(K key, PartialSearchMode partialSearchMode) throws IOException {
    long pageIndex = ROOT_INDEX;
    final ArrayList<Long> path = new ArrayList<Long>();

    if (!(keySize == 1 || ((OCompositeKey) key).getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey((Comparable<? super K>) key);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else
        keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++)
        fullKey.addKey(keyItem);

      key = (K) fullKey;
    }

    while (true) {
      path.add(pageIndex);
      final OCacheEntry bucketEntry = diskCache.load(fileId, pageIndex, false);
      final OCachePointer bucketPointer = bucketEntry.getCachePointer();

      final OSBTreeBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(bucketPointer.getDataPointer(), keySerializer,
            valueSerializer, ODurablePage.TrackMode.NONE);
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
    }
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

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether lowest
   * or highest partially matched key should be used.
   * 
   * 
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

  private static final class PagePathItemUnit {
    private final long pageIndex;
    private final int  itemIndex;

    private PagePathItemUnit(long pageIndex, int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  public static interface RangeResultListener<K, V> {
    public boolean addResult(OSBTreeBucket.SBTreeEntry<K, V> entry);
  }

}
