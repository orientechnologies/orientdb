package com.orientechnologies.orient.core.index.sbtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OLocalSBTree<K> extends OSharedResourceAdaptive {
  private final static long           ROOT_INDEX = 0;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private OStorageLocalAbstract       storage;
  private String                      name;

  private final String                dataFileExtension;

  private ODiskCache                  diskCache;

  private long                        fileId;

  private OBinarySerializer<K>        keySerializer;

  public OLocalSBTree(String dataFileExtension) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
  }

  public void create(String name, OBinarySerializer<K> keySerializer, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;

      fileId = diskCache.openFile(name + dataFileExtension);

      long rootPointer = diskCache.load(fileId, ROOT_INDEX);
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer, true, keySerializer);
        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setTreeSize(0);
      } finally {
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
      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.index < 0)
        return null;

      long keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
      try {
        OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer, keySerializer);
        return keyBucket.getEntry(bucketSearchResult.index).value;
      } finally {
        diskCache.release(fileId, bucketSearchResult.pageIndex);
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
      BucketSearchResult bucketSearchResult = findBucket(key);

      long keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
      OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer, keySerializer);

      if (bucketSearchResult.index >= 0) {
        keyBucket.updateValue(bucketSearchResult.index, value);
      } else {
        int insertionIndex = -bucketSearchResult.index - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBucket.SBTreeEntry<K>(-1, -1, key, value))) {
          diskCache.release(fileId, bucketSearchResult.pageIndex);
          bucketSearchResult = splitBucket(bucketSearchResult.pageIndex, insertionIndex);

          insertionIndex = bucketSearchResult.index;

          keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
          keyBucket = new OSBTreeBucket<K>(keyBucketPointer, keySerializer);
        }
      }

      diskCache.markDirty(fileId, bucketSearchResult.pageIndex);
      diskCache.release(fileId, bucketSearchResult.pageIndex);

      setSize(size() + 1);
    } catch (IOException e) {
      throw new OIndexException("Error during index update", e);
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

        long rootPointer = diskCache.load(fileId, ROOT_INDEX);
        try {
          OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer, true, keySerializer);
          rootBucket.setKeySerializerId(keySerializer.getId());
          rootBucket.setTreeSize(0);
        } finally {
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

      long rootPointer = diskCache.load(fileId, ROOT_INDEX);
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer, true, keySerializer);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getKeySerializerId());
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
    long rootPointer = diskCache.load(fileId, ROOT_INDEX);
    try {
      OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer, keySerializer);
      rootBucket.setTreeSize(size);
      diskCache.markDirty(fileId, ROOT_INDEX);
    } finally {
      diskCache.release(fileId, ROOT_INDEX);
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      long rootPointer = diskCache.load(fileId, ROOT_INDEX);
      try {
        OSBTreeBucket<K> rootBucket = new OSBTreeBucket<K>(rootPointer, keySerializer);
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
      BucketSearchResult bucketSearchResult = findBucket(key);
      if (bucketSearchResult.index < 0)
        return null;

      long keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
      try {
        OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(keyBucketPointer, keySerializer);

        final ORID removed = keyBucket.getEntry(bucketSearchResult.index).value;

        keyBucket.remove(bucketSearchResult.index);
        return removed;
      } finally {
        diskCache.markDirty(fileId, bucketSearchResult.pageIndex);
        diskCache.release(fileId, bucketSearchResult.pageIndex);
      }
    } catch (IOException e) {
      throw new OIndexException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private BucketSearchResult splitBucket(long pageIndex, int keyIndex) throws IOException {
    long bucketPointer = diskCache.load(fileId, pageIndex);
    try {
      OSBTreeBucket<K> bucketToSplit = new OSBTreeBucket<K>(bucketPointer, keySerializer);

      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final List<OSBTreeBucket.SBTreeEntry<K>> rightEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K>>(indexToSplit);

      final int startRightIndex = bucketToSplit.isLeaf() ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (pageIndex != ROOT_INDEX) {
        long rightBucketPageIndex = diskCache.getFilledUpTo(fileId);
        long rightBucketPointer = diskCache.load(fileId, rightBucketPageIndex);
        try {
          OSBTreeBucket<K> newRightBucket = new OSBTreeBucket<K>(rightBucketPointer, bucketToSplit.isLeaf(), keySerializer);
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          long parentIndex = bucketToSplit.getParent();
          long parentPointer = diskCache.load(fileId, parentIndex);
          try {
            OSBTreeBucket<K> parentBucket = new OSBTreeBucket<K>(parentPointer, keySerializer);

            OSBTreeBucket.SBTreeEntry<K> parentEntry = new OSBTreeBucket.SBTreeEntry<K>(pageIndex, rightBucketPageIndex,
                rightEntries.get(0).key, null);

            int insertionIndex = parentBucket.find(parentEntry.key);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry)) {
              diskCache.release(fileId, parentIndex);
              BucketSearchResult bucketSearchResult = splitBucket(parentIndex, insertionIndex);

              parentIndex = bucketSearchResult.pageIndex;
              parentPointer = diskCache.load(fileId, parentIndex);
              insertionIndex = bucketSearchResult.index;

              parentBucket = new OSBTreeBucket<K>(parentPointer, keySerializer);
            }

            bucketToSplit.setParent(parentIndex);
            newRightBucket.setParent(parentIndex);
          } finally {
            diskCache.markDirty(fileId, parentIndex);
            diskCache.release(fileId, parentIndex);
          }
        } finally {
          diskCache.markDirty(fileId, rightBucketPageIndex);
          diskCache.release(fileId, rightBucketPageIndex);
        }
        if (keyIndex < indexToSplit)
          return new BucketSearchResult(keyIndex, pageIndex);

        return new BucketSearchResult(keyIndex - indexToSplit, rightBucketPageIndex);
      } else {
        final List<OSBTreeBucket.SBTreeEntry<K>> leftEntries = new ArrayList<OSBTreeBucket.SBTreeEntry<K>>(indexToSplit);

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        long leftBucketPageIndex = diskCache.getFilledUpTo(fileId);
        long leftBucketPointer = diskCache.load(fileId, leftBucketPageIndex);

        try {
          OSBTreeBucket<K> newLeftBucket = new OSBTreeBucket<K>(leftBucketPointer, bucketToSplit.isLeaf(), keySerializer);
          newLeftBucket.addAll(leftEntries);

          newLeftBucket.setParent(pageIndex);
          diskCache.markDirty(fileId, leftBucketPageIndex);
        } finally {
          diskCache.release(fileId, leftBucketPageIndex);
        }

        long rightBucketPageIndex = diskCache.getFilledUpTo(fileId);
        long rightBucketPointer = diskCache.load(fileId, rightBucketPageIndex);
        try {
          OSBTreeBucket<K> newRightBucket = new OSBTreeBucket<K>(rightBucketPointer, bucketToSplit.isLeaf(), keySerializer);
          newRightBucket.addAll(rightEntries);

          newRightBucket.setParent(pageIndex);
          diskCache.markDirty(fileId, rightBucketPageIndex);
        } finally {
          diskCache.release(fileId, rightBucketPageIndex);
        }

        bucketToSplit = new OSBTreeBucket<K>(bucketPointer, false, keySerializer);
        bucketToSplit.addEntry(0, new OSBTreeBucket.SBTreeEntry<K>(leftBucketPageIndex, rightBucketPageIndex,
            rightEntries.get(0).key, null));

        if (keyIndex < indexToSplit)
          return new BucketSearchResult(keyIndex, leftBucketPageIndex);

        return new BucketSearchResult(keyIndex - indexToSplit, rightBucketPageIndex);
      }
    } finally {
      diskCache.markDirty(fileId, pageIndex);
      diskCache.release(fileId, pageIndex);
    }
  }

  private BucketSearchResult findBucket(K key) throws IOException {
    long pageIndex = ROOT_INDEX;

    while (true) {
      final long bucketPointer = diskCache.load(fileId, pageIndex);
      final OSBTreeBucket.SBTreeEntry<K> entry;
      try {
        final OSBTreeBucket<K> keyBucket = new OSBTreeBucket<K>(bucketPointer, keySerializer);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf())
          return new BucketSearchResult(index, pageIndex);

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
    private final int  index;
    private final long pageIndex;

    private BucketSearchResult(int index, long pageIndex) {
      this.index = index;
      this.pageIndex = pageIndex;
    }
  }
}
