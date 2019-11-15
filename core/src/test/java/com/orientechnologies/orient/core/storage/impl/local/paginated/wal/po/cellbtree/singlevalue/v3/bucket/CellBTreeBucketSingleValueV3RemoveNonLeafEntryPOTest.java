package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueBucketV3;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeBucketSingleValueV3RemoveNonLeafEntryPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(entry);
      bucket.init(false);

      bucket.addNonLeafEntry(0, 1, 2, new byte[] { 0 }, true);
      bucket.addNonLeafEntry(1, 2, 3, new byte[] { 1 }, true);
      bucket.addNonLeafEntry(2, 3, 4, new byte[] { 2 }, true);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeNonLeafEntry(1, new byte[] { 1 }, 3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO);

      final CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO pageOperation = (CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO) operations
          .get(0);

      CellBTreeSingleValueBucketV3<Byte> restoredBucket = new CellBTreeSingleValueBucketV3<>(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(1, 2, (byte) 0, null),
          restoredBucket.getEntry(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(2, 3, (byte) 1, null),
          restoredBucket.getEntry(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(3, 4, (byte) 2, null),
          restoredBucket.getEntry(2, OByteSerializer.INSTANCE));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(2, restoredBucket.size());

      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(1, 3, (byte) 0, null),
          restoredBucket.getEntry(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(3, 4, (byte) 2, null),
          restoredBucket.getEntry(1, OByteSerializer.INSTANCE));

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

      CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(entry);
      bucket.init(false);

      bucket.addNonLeafEntry(0, 1, 2, new byte[] { 0 }, true);
      bucket.addNonLeafEntry(1, 2, 3, new byte[] { 1 }, true);
      bucket.addNonLeafEntry(2, 3, 4, new byte[] { 2 }, true);

      entry.clearPageOperations();

      bucket.removeNonLeafEntry(1, new byte[] { 1 }, 3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO);

      final CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO pageOperation = (CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO) operations
          .get(0);

      final CellBTreeSingleValueBucketV3<Byte> restoredBucket = new CellBTreeSingleValueBucketV3<>(entry);

      Assert.assertEquals(2, restoredBucket.size());

      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(1, 3, (byte) 0, null),
          restoredBucket.getEntry(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(3, 4, (byte) 2, null),
          restoredBucket.getEntry(1, OByteSerializer.INSTANCE));

      pageOperation.undo(entry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(1, 2, (byte) 0, null),
          restoredBucket.getEntry(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(2, 3, (byte) 1, null),
          restoredBucket.getEntry(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(new CellBTreeSingleValueBucketV3.CellBTreeEntry<>(3, 4, (byte) 2, null),
          restoredBucket.getEntry(2, OByteSerializer.INSTANCE));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO operation = new CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO(12, 21,
        new byte[] { 4, 2 }, 42, 24);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO restoredOperation = new CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getIndex());
    Assert.assertEquals(21, restoredOperation.getPrevChild());
    Assert.assertArrayEquals(new byte[] { 4, 2 }, restoredOperation.getKey());
    Assert.assertEquals(42, restoredOperation.getLeftChild());
    Assert.assertEquals(24, restoredOperation.getRightChild());
  }
}
