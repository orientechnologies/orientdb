package com.orientechnologies.orient.core.storage.index.sbtree.local.v1;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class OSBTreeNullBucketV1Test {
  @Test
  public void testEmptyBucket() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeNullBucketV1<String> bucket = new OSBTreeNullBucketV1<>(cacheEntry);
    bucket.init();

    Assert.assertNull(bucket.getValue(OStringSerializer.INSTANCE));

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddGetValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeNullBucketV1<String> bucket = new OSBTreeNullBucketV1<>(cacheEntry);
    bucket.init();

    bucket.setValue(
        OStringSerializer.INSTANCE.serializeNativeAsWhole("test"), OStringSerializer.INSTANCE);
    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNotNull(treeValue);
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeNullBucketV1<String> bucket = new OSBTreeNullBucketV1<>(cacheEntry);
    bucket.init();

    bucket.setValue(
        OStringSerializer.INSTANCE.serializeNativeAsWhole("test"), OStringSerializer.INSTANCE);
    bucket.removeValue(OStringSerializer.INSTANCE);

    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveAddValue() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    OSBTreeNullBucketV1<String> bucket = new OSBTreeNullBucketV1<>(cacheEntry);
    bucket.init();

    bucket.setValue(
        OStringSerializer.INSTANCE.serializeNativeAsWhole("test"), OStringSerializer.INSTANCE);
    bucket.removeValue(OStringSerializer.INSTANCE);

    OSBTreeValue<String> treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNull(treeValue);

    bucket.setValue(
        OStringSerializer.INSTANCE.serializeNativeAsWhole("testOne"), OStringSerializer.INSTANCE);

    treeValue = bucket.getValue(OStringSerializer.INSTANCE);
    Assert.assertNotNull(treeValue);
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }
}
