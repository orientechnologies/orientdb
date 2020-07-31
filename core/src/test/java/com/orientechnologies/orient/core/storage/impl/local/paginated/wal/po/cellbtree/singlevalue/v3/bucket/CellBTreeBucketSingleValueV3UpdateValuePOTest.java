package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueBucketV3;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CellBTreeBucketSingleValueV3UpdateValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(entry);
      bucket.init(true);

      bucket.addLeafEntry(0, new byte[] {0}, serializeRid(new ORecordId(0, 0)));
      bucket.addLeafEntry(1, new byte[] {1}, serializeRid(new ORecordId(1, 1)));
      bucket.addLeafEntry(2, new byte[] {2}, serializeRid(new ORecordId(2, 2)));

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

      bucket.updateValue(1, serializeRid(new ORecordId(3, 3)), 1);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeBucketSingleValueV3UpdateValuePO);

      final CellBTreeBucketSingleValueV3UpdateValuePO pageOperation =
          (CellBTreeBucketSingleValueV3UpdateValuePO) operations.get(0);

      CellBTreeSingleValueBucketV3<Byte> restoredBucket =
          new CellBTreeSingleValueBucketV3<>(restoredCacheEntry);
      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0), restoredBucket.getValue(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(1, 1), restoredBucket.getValue(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(2, 2), restoredBucket.getValue(2, OByteSerializer.INSTANCE));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0), restoredBucket.getValue(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(3, 3), restoredBucket.getValue(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(2, 2), restoredBucket.getValue(2, OByteSerializer.INSTANCE));

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

      CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(entry);
      bucket.init(true);

      bucket.addLeafEntry(0, new byte[] {0}, serializeRid(new ORecordId(0, 0)));
      bucket.addLeafEntry(1, new byte[] {1}, serializeRid(new ORecordId(1, 1)));
      bucket.addLeafEntry(2, new byte[] {2}, serializeRid(new ORecordId(2, 2)));

      entry.clearPageOperations();

      bucket.updateValue(1, serializeRid(new ORecordId(3, 3)), 1);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeBucketSingleValueV3UpdateValuePO);

      final CellBTreeBucketSingleValueV3UpdateValuePO pageOperation =
          (CellBTreeBucketSingleValueV3UpdateValuePO) operations.get(0);

      final CellBTreeSingleValueBucketV3<Byte> restoredBucket =
          new CellBTreeSingleValueBucketV3<>(entry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0), restoredBucket.getValue(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(3, 3), restoredBucket.getValue(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(2, 2), restoredBucket.getValue(2, OByteSerializer.INSTANCE));

      pageOperation.undo(entry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0), restoredBucket.getValue(0, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(1, 1), restoredBucket.getValue(1, OByteSerializer.INSTANCE));
      Assert.assertEquals(
          new ORecordId(2, 2), restoredBucket.getValue(2, OByteSerializer.INSTANCE));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    CellBTreeBucketSingleValueV3UpdateValuePO operation =
        new CellBTreeBucketSingleValueV3UpdateValuePO(1, 23, new byte[] {2, 4}, new byte[] {4, 2});

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeBucketSingleValueV3UpdateValuePO restoredOperation =
        new CellBTreeBucketSingleValueV3UpdateValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(1, restoredOperation.getIndex());
    Assert.assertEquals(23, restoredOperation.getKeySize());
    Assert.assertArrayEquals(new byte[] {2, 4}, restoredOperation.getPrevValue());
    Assert.assertArrayEquals(new byte[] {4, 2}, restoredOperation.getValue());
  }

  private static byte[] serializeRid(ORID rid) {
    final ByteBuffer buffer = ByteBuffer.allocate(10).order(ByteOrder.nativeOrder());
    buffer.putShort((short) rid.getClusterId());
    buffer.putLong(rid.getClusterPosition());

    return buffer.array();
  }
}
