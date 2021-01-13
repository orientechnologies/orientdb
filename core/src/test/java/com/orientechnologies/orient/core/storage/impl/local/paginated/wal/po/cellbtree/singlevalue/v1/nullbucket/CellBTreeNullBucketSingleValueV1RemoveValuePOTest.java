package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeNullBucketSingleValueV1;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CellBTreeNullBucketSingleValueV1RemoveValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      CellBTreeNullBucketSingleValueV1 bucket = new CellBTreeNullBucketSingleValueV1(entry);
      bucket.init();

      bucket.setValue(new ORecordId(2, 2));

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer, false);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeValue();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV1RemoveValuePO);

      final CellBTreeNullBucketSingleValueV1RemoveValuePO pageOperation =
          (CellBTreeNullBucketSingleValueV1RemoveValuePO) operations.get(0);

      CellBTreeNullBucketSingleValueV1 restoredBucket =
          new CellBTreeNullBucketSingleValueV1(restoredCacheEntry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertNull(restoredBucket.getValue());

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
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      CellBTreeNullBucketSingleValueV1 bucket = new CellBTreeNullBucketSingleValueV1(entry);
      bucket.init();

      bucket.setValue(new ORecordId(2, 2));

      entry.clearPageOperations();

      bucket.removeValue();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV1RemoveValuePO);

      final CellBTreeNullBucketSingleValueV1RemoveValuePO pageOperation =
          (CellBTreeNullBucketSingleValueV1RemoveValuePO) operations.get(0);

      final CellBTreeNullBucketSingleValueV1 restoredBucket =
          new CellBTreeNullBucketSingleValueV1(entry);

      Assert.assertNull(restoredBucket.getValue());

      pageOperation.undo(entry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    CellBTreeNullBucketSingleValueV1RemoveValuePO operation =
        new CellBTreeNullBucketSingleValueV1RemoveValuePO(new ORecordId(2, 2));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeNullBucketSingleValueV1RemoveValuePO restoredOperation =
        new CellBTreeNullBucketSingleValueV1RemoveValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(new ORecordId(2, 2), restoredOperation.getValue());
  }
}
