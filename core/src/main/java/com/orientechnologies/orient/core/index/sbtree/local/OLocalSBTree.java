package com.orientechnologies.orient.core.index.sbtree.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.orientechnologies.common.collection.OAlwaysGreaterKey;
import com.orientechnologies.common.collection.OAlwaysLessKey;
import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OLocalSBTree<K> extends OSharedResourceAdaptive {
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

  public OLocalSBTree(String dataFileExtension, int keySize) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
    this.keySize = keySize;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;

      fileId = diskCache.openFile(name + dataFileExtension);

      OCachePointer rootPointer = diskCache.load(fileId, ROOT_INDEX);
      rootPointer.acquireExclusiveLock();
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer.getDataPointer(), true, keySerializer);
        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setTreeSize(0);
        rootBucket.setKeySize((byte) keySize);

        diskCache.markDirty(fileId, ROOT_INDEX);
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(fileId, ROOT_INDEX);
      }

    } catch (IOException e) {
      throw new OIndexException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void open(String name, OBinarySerializer<K> keySerializer, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;

      fileId = diskCache.openFile(name + dataFileExtension);
    } catch (IOException e) {
      throw new OIndexException("Error during open of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public ORID get(K key) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.index < 0)
        return null;

      long pageIndex = bucketSearchResult.getLastPathItem();
      OCachePointer keyBucketPointer = diskCache.load(fileId, pageIndex);
      try {
        OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer.getDataPointer(), keySerializer);
        return keyBucket.getEntry(bucketSearchResult.index).value;
      } finally {
        diskCache.release(fileId, pageIndex);
      }

    } catch (IOException e) {
      throw new OIndexException("Error during retrieving  of sbtree with name " + name, e);
    } finally {
      releaseSharedLock();
    }
  }

  public void put(K key, ORID value) {
    acquireExclusiveLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);

      OCachePointer keyBucketPointer = diskCache.load(fileId, bucketSearchResult.getLastPathItem());
      keyBucketPointer.acquireExclusiveLock();
      OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer.getDataPointer(), keySerializer);

      if (bucketSearchResult.index >= 0) {
        keyBucket.updateValue(bucketSearchResult.index, value);
      } else {
        int insertionIndex = -bucketSearchResult.index - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBucket.SBTreeEntry<K>(-1, -1, key, value), true)) {
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(fileId, bucketSearchResult.getLastPathItem());

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);

          insertionIndex = bucketSearchResult.index;

          keyBucketPointer = diskCache.load(fileId, bucketSearchResult.getLastPathItem());
          keyBucketPointer.acquireExclusiveLock();
          keyBucket = new OSBTreeBucket<K>(keyBucketPointer.getDataPointer(), keySerializer);
        }
      }

      keyBucketPointer.releaseExclusiveLock();
      diskCache.markDirty(fileId, bucketSearchResult.getLastPathItem());
      diskCache.release(fileId, bucketSearchResult.getLastPathItem());

      setSize(size() + 1);
    } catch (IOException e) {
      throw new OIndexException("Error during index update with key " + key + " and value " + value, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void close(boolean flush) {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId, flush);
    } catch (IOException e) {
      throw new OIndexException("Error during close of index " + name, e);
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
      try {
        diskCache.truncateFile(fileId);

        OCachePointer rootPointer = diskCache.load(fileId, ROOT_INDEX);
        rootPointer.acquireExclusiveLock();
        try {
          OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer.getDataPointer(), true, keySerializer);

          rootBucket.setKeySerializerId(keySerializer.getId());
          rootBucket.setTreeSize(0);

          diskCache.markDirty(fileId, ROOT_INDEX);
        } finally {
          rootPointer.releaseExclusiveLock();
          diskCache.release(fileId, ROOT_INDEX);
        }
      } catch (IOException e) {
        throw new OIndexException("Error during clear of sbtree with name " + name, e);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } catch (IOException e) {
      throw new OIndexException("Error during delete of sbtree with name " + name, e);
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

      OCachePointer rootPointer = diskCache.load(fileId, ROOT_INDEX);
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer.getDataPointer(), true, keySerializer);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getKeySerializerId());
        keySize = rootBucket.getKeySize();
      } finally {
        diskCache.release(fileId, ROOT_INDEX);
      }
    } catch (IOException e) {
      throw new OIndexException("Exception during loading of sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void setSize(long size) throws IOException {
    OCachePointer rootPointer = diskCache.load(fileId, ROOT_INDEX);
    rootPointer.acquireExclusiveLock();
    try {
      OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer.getDataPointer(), keySerializer);
      rootBucket.setTreeSize(size);
      diskCache.markDirty(fileId, ROOT_INDEX);
    } finally {
      rootPointer.releaseExclusiveLock();
      diskCache.release(fileId, ROOT_INDEX);
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      OCachePointer rootPointer = diskCache.load(fileId, ROOT_INDEX);
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer.getDataPointer(), keySerializer);
        return rootBucket.getTreeSize();
      } finally {
        diskCache.release(fileId, ROOT_INDEX);
      }
    } catch (IOException e) {
      throw new OIndexException("Error during retrieving of size of index " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public ORID remove(K key) {
    acquireExclusiveLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.index < 0)
        return null;

      OCachePointer keyBucketPointer = diskCache.load(fileId, bucketSearchResult.getLastPathItem());
      keyBucketPointer.acquireExclusiveLock();
      try {
        OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer.getDataPointer(), keySerializer);

        final ORID removed = keyBucket.getEntry(bucketSearchResult.index).value;

        keyBucket.remove(bucketSearchResult.index);
        diskCache.markDirty(fileId, bucketSearchResult.getLastPathItem());

        return removed;
      } finally {
        keyBucketPointer.releaseExclusiveLock();
        diskCache.release(fileId, bucketSearchResult.getLastPathItem());
      }
    } catch (IOException e) {
      throw new OIndexException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public Collection<ORID> getValuesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<ORID> result = new ArrayList<ORID>();

    loadValuesMinor(key, inclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public Collection<ODocument> getEntitiesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<ODocument> result = new ArrayList<ODocument>();

    loadValuesMinor(key, inclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        final ODocument document = new ODocument();
        document.field("key", entry.key);
        document.field("rid", entry.value);
        document.unsetDirty();

        result.add(document);

        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  private void loadValuesMinor(K key, boolean inclusive, RangeResultListener<K> listener) {
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
      if (bucketSearchResult.index >= 0) {
        index = inclusive ? bucketSearchResult.index : bucketSearchResult.index - 1;
      } else {
        index = -bucketSearchResult.index - 2;
      }

      boolean firstBucket = true;
      resultsLoop: while (true) {
        long nextPageIndex = -1;
        final OCachePointer pointer = diskCache.load(fileId, pageIndex);
        try {
          OSBTreeBucket<K> bucket = new OSBTreeBucket<K>(pointer.getDataPointer(), keySerializer);
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
          diskCache.release(fileId, pageIndex);
        }

        pageIndex = nextPageIndex;
        firstBucket = false;
      }
    } catch (IOException ioe) {
      throw new OIndexException("Error during fetch of minor values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ORID> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<ORID> result = new ArrayList<ORID>();

    loadValuesMajor(key, inclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public Collection<ODocument> getEntriesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<ODocument> result = new ArrayList<ODocument>();

    loadValuesMajor(key, inclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        final ODocument document = new ODocument();
        document.field("key", entry.key);
        document.field("rid", entry.value);
        document.unsetDirty();

        result.add(document);

        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  private void loadValuesMajor(K key, boolean inclusive, RangeResultListener<K> listener) {
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
      if (bucketSearchResult.index >= 0) {
        index = inclusive ? bucketSearchResult.index : bucketSearchResult.index + 1;
      } else {
        index = -bucketSearchResult.index - 1;
      }

      resultsLoop: while (true) {
        long nextPageIndex = -1;
        final OCachePointer pointer = diskCache.load(fileId, pageIndex);
        try {
          OSBTreeBucket<K> bucket = new OSBTreeBucket<K>(pointer.getDataPointer(), keySerializer);
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
          diskCache.release(fileId, pageIndex);
        }

        pageIndex = nextPageIndex;
        index = 0;
      }

    } catch (IOException ioe) {
      throw new OIndexException("Error during fetch of major values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ORID> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      final int maxValuesToFetch) {
    final List<ORID> result = new ArrayList<ORID>();
    loadEntriesBetween(keyFrom, fromInclusive, keyTo, toInclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > 0 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public Collection<ODocument> getEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      final int maxValuesToFetch) {
    final List<ODocument> result = new ArrayList<ODocument>();
    loadEntriesBetween(keyFrom, fromInclusive, keyTo, toInclusive, new RangeResultListener<K>() {
      @Override
      public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry) {
        final ODocument document = new ODocument();
        document.field("key", entry.key);
        document.field("rid", entry.value);
        document.unsetDirty();

        result.add(document);

        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  private void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K> listener) {
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
      if (bucketSearchResultFrom.index >= 0) {
        indexFrom = fromInclusive ? bucketSearchResultFrom.index : bucketSearchResultFrom.index + 1;
      } else {
        indexFrom = -bucketSearchResultFrom.index - 1;
      }

      PartialSearchMode partialSearchModeTo;
      if (toInclusive)
        partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
      else
        partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

      BucketSearchResult bucketSearchResultTo = findBucket(keyTo, partialSearchModeTo);
      long pageIndexTo = bucketSearchResultTo.getLastPathItem();

      int indexTo;
      if (bucketSearchResultTo.index >= 0) {
        indexTo = toInclusive ? bucketSearchResultTo.index : bucketSearchResultTo.index - 1;
      } else {
        indexTo = -bucketSearchResultTo.index - 2;
      }

      int startIndex = indexFrom;
      int endIndex;
      long pageIndex = pageIndexFrom;

      resultsLoop: while (true) {
        long nextPageIndex = -1;
        final OCachePointer pointer = diskCache.load(fileId, pageIndex);
        try {
          OSBTreeBucket<K> bucket = new OSBTreeBucket<K>(pointer.getDataPointer(), keySerializer);
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
          diskCache.release(fileId, pageIndex);
        }

        pageIndex = nextPageIndex;
        startIndex = 0;
      }

    } catch (IOException ioe) {
      throw new OIndexException("Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  private BucketSearchResult splitBucket(List<Long> path, int keyIndex, K keyToInsert) throws IOException {
    long pageIndex = path.get(path.size() - 1);
    OCachePointer bucketPointer = diskCache.load(fileId, pageIndex);

    bucketPointer.acquireExclusiveLock();
    try {
      OSBTreeBucket<K> bucketToSplit = new OSBTreeBucket<K>(bucketPointer.getDataPointer(), keySerializer);

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBucket.SBTreeEntry<K>> rightEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K>>(indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (pageIndex != ROOT_INDEX) {
        long rightBucketPageIndex = diskCache.getFilledUpTo(fileId);
        OCachePointer rightBucketPointer = diskCache.load(fileId, rightBucketPageIndex);
        rightBucketPointer.acquireExclusiveLock();

        try {
          OSBTreeBucket<K> newRightBucket = new OSBTreeBucket<K>(rightBucketPointer.getDataPointer(), splitLeaf, keySerializer);
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            long rightSiblingPageIndex = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingPageIndex);
            newRightBucket.setLeftSibling(pageIndex);

            bucketToSplit.setRightSibling(rightBucketPageIndex);

            if (rightSiblingPageIndex >= 0) {
              final OCachePointer rightSiblingPointer = diskCache.load(fileId, rightSiblingPageIndex);
              rightSiblingPointer.acquireExclusiveLock();
              OSBTreeBucket<K> rightSiblingBucket = new OSBTreeBucket<K>(rightSiblingPointer.getDataPointer(), keySerializer);
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPageIndex);
                diskCache.markDirty(fileId, rightSiblingPageIndex);
              } finally {
                rightSiblingPointer.releaseExclusiveLock();
                diskCache.release(fileId, rightSiblingPageIndex);
              }
            }
          }

          long parentIndex = path.get(path.size() - 2);
          OCachePointer parentPointer = diskCache.load(fileId, parentIndex);
          parentPointer.acquireExclusiveLock();
          try {
            OSBTreeBucket<K> parentBucket = new OSBTreeBucket<K>(parentPointer.getDataPointer(), keySerializer);
            OSBTreeBucket.SBTreeEntry<K> parentEntry = new OSBTreeBucket.SBTreeEntry<K>(pageIndex, rightBucketPageIndex,
                separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentPointer.releaseExclusiveLock();
              diskCache.release(fileId, parentIndex);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

              parentIndex = bucketSearchResult.getLastPathItem();
              parentPointer = diskCache.load(fileId, parentIndex);
              parentPointer.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.index;

              parentBucket = new OSBTreeBucket<K>(parentPointer.getDataPointer(), keySerializer);
            }
          } finally {
            parentPointer.releaseExclusiveLock();
            diskCache.markDirty(fileId, parentIndex);
            diskCache.release(fileId, parentIndex);
          }
        } finally {
          rightBucketPointer.releaseExclusiveLock();
          diskCache.markDirty(fileId, rightBucketPageIndex);
          diskCache.release(fileId, rightBucketPageIndex);
        }

        ArrayList<Long> resultPath = new ArrayList<Long>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(pageIndex);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPageIndex);
        if (splitLeaf) {
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
        }

        resultPath.add(rightBucketPageIndex);
        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);

      } else {
        final List<OSBTreeBucket.SBTreeEntry<K>> leftEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K>>(indexToSplit);

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        long leftBucketPageIndex = diskCache.getFilledUpTo(fileId);
        OCachePointer leftBucketPointer = diskCache.load(fileId, leftBucketPageIndex);

        long rightBucketPageIndex = diskCache.getFilledUpTo(fileId);
        leftBucketPointer.acquireExclusiveLock();
        try {
          OSBTreeBucket<K> newLeftBucket = new OSBTreeBucket<K>(leftBucketPointer.getDataPointer(), splitLeaf, keySerializer);
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketPageIndex);

          diskCache.markDirty(fileId, leftBucketPageIndex);
        } finally {
          leftBucketPointer.releaseExclusiveLock();
          diskCache.release(fileId, leftBucketPageIndex);
        }

        OCachePointer rightBucketPointer = diskCache.load(fileId, rightBucketPageIndex);
        rightBucketPointer.acquireExclusiveLock();
        try {
          OSBTreeBucket<K> newRightBucket = new OSBTreeBucket<K>(rightBucketPointer.getDataPointer(), splitLeaf, keySerializer);
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketPageIndex);

          diskCache.markDirty(fileId, rightBucketPageIndex);
        } finally {
          rightBucketPointer.releaseExclusiveLock();
          diskCache.release(fileId, rightBucketPageIndex);
        }

        bucketToSplit = new OSBTreeBucket<K>(bucketPointer.getDataPointer(), false, keySerializer);
        bucketToSplit.addEntry(0, new OSBTreeBucket.SBTreeEntry<K>(leftBucketPageIndex, rightBucketPageIndex, separationKey, null),
            true);

        ArrayList<Long> resultPath = new ArrayList<Long>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(leftBucketPageIndex);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPageIndex);

        if (splitLeaf)
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
      }

    } finally {
      bucketPointer.releaseExclusiveLock();

      diskCache.markDirty(fileId, pageIndex);
      diskCache.release(fileId, pageIndex);
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
      final OCachePointer bucketPointer = diskCache.load(fileId, pageIndex);
      final OSBTreeBucket.SBTreeEntry<K> entry;
      try {
        final OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(bucketPointer.getDataPointer(), keySerializer);
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
        diskCache.release(fileId, pageIndex);
      }

      if (comparator.compare(key, entry.key) >= 0)
        pageIndex = entry.rightChild;
      else
        pageIndex = entry.leftChild;
    }
  }

  private static class BucketSearchResult {
    private final int             index;
    private final ArrayList<Long> path;

    private BucketSearchResult(int index, ArrayList<Long> path) {
      this.index = index;
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

  private static interface RangeResultListener<K> {
    public boolean addResult(OSBTreeBucket.SBTreeEntry<K> entry);
  }

}
