package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2NullBucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeMultiValueV2NullBucketRemoveValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeMultiValueV2NullBucket bucket = new CellBTreeMultiValueV2NullBucket(entry);
      bucket.init(12);

      bucket.addValue(new ORecordId(23, 45));

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeValue(new ORecordId(23, 45));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2NullBucketRemoveValuePO);

      final CellBTreeMultiValueV2NullBucketRemoveValuePO pageOperation = (CellBTreeMultiValueV2NullBucketRemoveValuePO) operations
          .get(0);

      CellBTreeMultiValueV2NullBucket restoredBucket = new CellBTreeMultiValueV2NullBucket(restoredCacheEntry);

      Assert.assertEquals(1, restoredBucket.getValues().size());
      Assert.assertEquals(new ORecordId(23, 45), restoredBucket.getValues().get(0));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertTrue(restoredBucket.getValues().isEmpty());

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

      CellBTreeMultiValueV2NullBucket bucket = new CellBTreeMultiValueV2NullBucket(entry);
      bucket.init(12);
      bucket.addValue(new ORecordId(23, 45));

      entry.clearPageOperations();

      bucket.removeValue(new ORecordId(23, 45));

      final List<PageOperationRecord> operations = entry.getPageOperations();

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2NullBucketRemoveValuePO);
      final CellBTreeMultiValueV2NullBucketRemoveValuePO pageOperation = (CellBTreeMultiValueV2NullBucketRemoveValuePO) operations
          .get(0);

      final CellBTreeMultiValueV2NullBucket restoredBucket = new CellBTreeMultiValueV2NullBucket(entry);

      Assert.assertTrue(restoredBucket.getValues().isEmpty());

      pageOperation.undo(entry);

      Assert.assertEquals(1, restoredBucket.getValues().size());
      Assert.assertEquals(new ORecordId(23, 45), restoredBucket.getValues().get(0));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeMultiValueV2NullBucketRemoveValuePO operation = new CellBTreeMultiValueV2NullBucketRemoveValuePO(
        new ORecordId(12, 34));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2NullBucketRemoveValuePO restoredOperation = new CellBTreeMultiValueV2NullBucketRemoveValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(new ORecordId(12, 34), restoredOperation.getRid());
  }
}
