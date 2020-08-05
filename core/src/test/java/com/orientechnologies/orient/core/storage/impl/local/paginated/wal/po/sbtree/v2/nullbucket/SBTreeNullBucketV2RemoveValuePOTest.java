package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeNullBucketV2;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeValue;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SBTreeNullBucketV2RemoveValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OSBTreeNullBucketV2<Byte> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(new byte[] {2}, OByteSerializer.INSTANCE);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer, false);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeValue(OByteSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2RemoveValuePO);

      final SBTreeNullBucketV2RemoveValuePO pageOperation =
          (SBTreeNullBucketV2RemoveValuePO) operations.get(0);

      OSBTreeNullBucketV2<Byte> restoredBucket = new OSBTreeNullBucketV2<>(restoredCacheEntry);

      final OSBTreeValue<Byte> value = restoredBucket.getValue(OByteSerializer.INSTANCE);
      Assert.assertNotNull(value);
      Assert.assertEquals((byte) 2, (byte) value.getValue());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertNull(restoredBucket.getValue(OByteSerializer.INSTANCE));

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndo() {
    final int pageSize = 64 * 1024;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OSBTreeNullBucketV2<Byte> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(new byte[] {2}, OByteSerializer.INSTANCE);

      entry.clearPageOperations();

      bucket.removeValue(OByteSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2RemoveValuePO);

      final SBTreeNullBucketV2RemoveValuePO pageOperation =
          (SBTreeNullBucketV2RemoveValuePO) operations.get(0);

      final OSBTreeNullBucketV2<Byte> restoredBucket = new OSBTreeNullBucketV2<>(entry);

      Assert.assertNull(restoredBucket.getValue(OByteSerializer.INSTANCE));

      pageOperation.undo(entry);

      final OSBTreeValue<Byte> value = restoredBucket.getValue(OByteSerializer.INSTANCE);
      Assert.assertNotNull(value);
      Assert.assertEquals((byte) 2, (byte) value.getValue());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    SBTreeNullBucketV2RemoveValuePO operation =
        new SBTreeNullBucketV2RemoveValuePO(new byte[] {1, 2, 3}, OByteSerializer.INSTANCE);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    SBTreeNullBucketV2RemoveValuePO restoredOperation = new SBTreeNullBucketV2RemoveValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertArrayEquals(new byte[] {1, 2, 3}, restoredOperation.getValue());
    Assert.assertSame(OByteSerializer.INSTANCE, restoredOperation.getValueSerializer());
  }
}
