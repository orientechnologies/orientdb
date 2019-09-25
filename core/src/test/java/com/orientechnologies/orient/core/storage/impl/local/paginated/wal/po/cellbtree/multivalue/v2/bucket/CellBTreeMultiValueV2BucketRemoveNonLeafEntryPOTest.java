package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
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

public class CellBTreeMultiValueV2BucketRemoveNonLeafEntryPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeMultiValueV2Bucket<Byte> bucket = new CellBTreeMultiValueV2Bucket<>(entry);
      bucket.init(false);

      bucket.addNonLeafEntry(0, new byte[] { 0 }, 1, 2, true);
      bucket.addNonLeafEntry(1, new byte[] { 1 }, 2, 3, true);
      bucket.addNonLeafEntry(2, new byte[] { 2 }, 3, 4, true);

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

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO);

      final CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO pageOperation = (CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO) operations
          .get(0);

      CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 0 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(2, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(2, restoredBucket.size());

      nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 0 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);

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
      bucket.init(false);

      bucket.addNonLeafEntry(0, new byte[] { 0 }, 1, 2, true);
      bucket.addNonLeafEntry(1, new byte[] { 1 }, 2, 3, true);
      bucket.addNonLeafEntry(2, new byte[] { 2 }, 3, 4, true);

      entry.clearPageOperations();

      bucket.removeNonLeafEntry(1, new byte[] { 1 }, 3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO);

      final CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO pageOperation = (CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO) operations
          .get(0);

      final CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(entry);

      Assert.assertEquals(2, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 0 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);
      pageOperation.undo(entry);

      Assert.assertEquals(3, restoredBucket.size());

      nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 0 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(2, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO operation = new CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO(24,
        new byte[] { 1, 2 }, 12, 21, 54);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO restoredOperation = new CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(24, restoredOperation.getIndex());
    Assert.assertArrayEquals(new byte[] { 1, 2 }, restoredOperation.getKey());
    Assert.assertEquals(12, restoredOperation.getLeft());
    Assert.assertEquals(21, restoredOperation.getRight());
    Assert.assertEquals(54, restoredOperation.getPrevChild());
  }
}
