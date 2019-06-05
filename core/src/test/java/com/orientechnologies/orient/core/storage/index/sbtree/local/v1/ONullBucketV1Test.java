package com.orientechnologies.orient.core.storage.index.sbtree.local.v1;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class ONullBucketV1Test {
  @Test
  public void testEmptyBucket() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddGetValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

  @Test
  public void testAddRemoveAddValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "testOne"));

    treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
    bufferPool.clear();
  }

}
