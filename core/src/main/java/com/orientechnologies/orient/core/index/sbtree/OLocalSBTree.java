package com.orientechnologies.orient.core.index.sbtree;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;

/**
 * @author Andrey Lomakin
 * @since 8/7/13
 */
public class OLocalSBTree<K, V> extends OSharedResourceAdaptive {
  private final String         dataFileExtension;

  private ODiskCache           diskCache;
  private long                 fileId;

  private OBinarySerializer<K> keySerializer;
  private OBinarySerializer<V> valueSerializer;

  public OLocalSBTree(String dataFileExtension) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
  }

  public void put(K key, V value) {
    acquireExclusiveLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key);

      long keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
      OSBTreeBucket<K, V> keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer, keySerializer, valueSerializer);

      if (bucketSearchResult.index >= 0) {
        keyBucket.remove(bucketSearchResult.index);
        diskCache.markDirty(fileId, bucketSearchResult.pageIndex);
      }

      while (!keyBucket.put(bucketSearchResult.index, key, value)) {
        if (keyBucket.size() == 0) {
          diskCache.release(fileId, bucketSearchResult.pageIndex);
          throw new OIndexException(
              "Key value pair can not be added in index because their serialized presentation exceed index page size");
        }

        diskCache.release(fileId, bucketSearchResult.pageIndex);
        bucketSearchResult = splitBucket(bucketSearchResult.pageIndex, bucketSearchResult.index);

        keyBucketPointer = diskCache.load(fileId, bucketSearchResult.pageIndex);
        keyBucket = new OSBTreeBucket<K, V>(keyBucketPointer, keySerializer, valueSerializer);
      }

      diskCache.markDirty(fileId, bucketSearchResult.pageIndex);
      diskCache.release(fileId, bucketSearchResult.pageIndex);
    } catch (IOException e) {
      throw new OIndexException("Error during index update", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private BucketSearchResult splitBucket(long pageIndex, int keyIndex) {
    return null;
  }

  private BucketSearchResult findBucket(K key) {
    return null;
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
