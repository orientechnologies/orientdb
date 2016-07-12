package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 4/15/14
 */
public class ONullBucketTest {
  @Test
  public void testEmptyBucket() {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddGetValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddRemoveValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<String>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<String>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddRemoveAddValue() throws IOException {
    OByteBufferPool bufferPool = new OByteBufferPool(1024);
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
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
  }

}
