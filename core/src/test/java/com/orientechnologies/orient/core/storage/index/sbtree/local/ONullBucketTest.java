package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class ONullBucketTest {
  @Test
  public void testEmptyBucket() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddGetValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    assert treeValue != null;
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveAddValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<>(false, -1, "testOne"));

    treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

}
