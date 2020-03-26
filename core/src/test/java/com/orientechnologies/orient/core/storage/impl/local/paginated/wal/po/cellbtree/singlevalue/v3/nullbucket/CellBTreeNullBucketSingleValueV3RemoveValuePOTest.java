package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3NullBucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeNullBucketSingleValueV3RemoveValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      bucket.setValue(new ORecordId(2, 2));

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeValue();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3RemoveValuePO);

      final CellBTreeNullBucketSingleValueV3RemoveValuePO pageOperation = (CellBTreeNullBucketSingleValueV3RemoveValuePO) operations
          .get(0);

      CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(restoredCacheEntry);

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
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      bucket.setValue(new ORecordId(2, 2));

      entry.clearPageOperations();

      bucket.removeValue();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3RemoveValuePO);

      final CellBTreeNullBucketSingleValueV3RemoveValuePO pageOperation = (CellBTreeNullBucketSingleValueV3RemoveValuePO) operations
          .get(0);

      final CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(entry);

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
    CellBTreeNullBucketSingleValueV3RemoveValuePO operation = new CellBTreeNullBucketSingleValueV3RemoveValuePO(
        new ORecordId(2, 2));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeNullBucketSingleValueV3RemoveValuePO restoredOperation = new CellBTreeNullBucketSingleValueV3RemoveValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(new ORecordId(2, 2), restoredOperation.getValue());
  }
}
