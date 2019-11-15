package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeMultiValueV2BucketAppendNewLeafEntryPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeMultiValueV2Bucket<Byte> bucket = new CellBTreeMultiValueV2Bucket<>(entry);
      bucket.init(true);

      bucket.createMainLeafEntry(0, new byte[] { 1 }, new ORecordId(1, 1), 1);
      bucket.createMainLeafEntry(1, new byte[] { 2 }, new ORecordId(2, 2), 2);
      bucket.createMainLeafEntry(2, new byte[] { 3 }, new ORecordId(3, 3), 3);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.appendNewLeafEntry(1, new ORecordId(4, 4));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketAppendNewLeafEntryPO);

      final CellBTreeMultiValueV2BucketAppendNewLeafEntryPO pageOperation = (CellBTreeMultiValueV2BucketAppendNewLeafEntryPO) operations
          .get(0);

      CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(restoredCacheEntry);
      Assert.assertEquals(3, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.LeafEntry leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));
      Assert.assertEquals(1, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(2, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(0));
      Assert.assertEquals(3, leafEntry.mId);

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.size());

      leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));
      Assert.assertEquals(1, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(2, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(4, 4), leafEntry.values.get(1));
      Assert.assertEquals(2, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(0));
      Assert.assertEquals(3, leafEntry.mId);

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

      CellBTreeMultiValueV2Bucket<Byte> bucket = new CellBTreeMultiValueV2Bucket<>(entry);
      bucket.init(true);

      bucket.createMainLeafEntry(0, new byte[] { 1 }, new ORecordId(1, 1), 1);
      bucket.createMainLeafEntry(1, new byte[] { 2 }, new ORecordId(2, 2), 2);
      bucket.createMainLeafEntry(2, new byte[] { 3 }, new ORecordId(3, 3), 3);

      entry.clearPageOperations();

      bucket.appendNewLeafEntry(1, new ORecordId(4, 4));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketAppendNewLeafEntryPO);

      final CellBTreeMultiValueV2BucketAppendNewLeafEntryPO pageOperation = (CellBTreeMultiValueV2BucketAppendNewLeafEntryPO) operations
          .get(0);

      final CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(entry);

      Assert.assertEquals(3, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.LeafEntry leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));
      Assert.assertEquals(1, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(2, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(4, 4), leafEntry.values.get(1));
      Assert.assertEquals(2, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(0));
      Assert.assertEquals(3, leafEntry.mId);

      pageOperation.undo(entry);

      leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));
      Assert.assertEquals(1, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(2, leafEntry.mId);

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(0));
      Assert.assertEquals(3, leafEntry.mId);

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeMultiValueV2BucketAppendNewLeafEntryPO operation = new CellBTreeMultiValueV2BucketAppendNewLeafEntryPO(1,
        new ORecordId(5, 5));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2BucketAppendNewLeafEntryPO restoredOperation = new CellBTreeMultiValueV2BucketAppendNewLeafEntryPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(1, restoredOperation.getIndex());
    Assert.assertEquals(new ORecordId(5, 5), restoredOperation.getValue());
  }
}
