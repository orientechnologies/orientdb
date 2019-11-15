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
import java.util.ArrayList;
import java.util.List;

public class CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPOTest {
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

      bucket.addNonLeafEntry(0, new byte[] { 1 }, 1, 2, true);

      final List<CellBTreeMultiValueV2Bucket.NonLeafEntry> nonLeafEntries = new ArrayList<>(3);

      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryOne = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 2 }, 2, 3);
      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryTwo = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 3 }, 3, 4);
      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryThree = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 4 }, 4, 5);

      nonLeafEntries.add(nonLeafEntryOne);
      nonLeafEntries.add(nonLeafEntryTwo);
      nonLeafEntries.add(nonLeafEntryThree);

      bucket.addAll(nonLeafEntries, OByteSerializer.INSTANCE, false);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.shrink(1, OByteSerializer.INSTANCE, false);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO);

      final CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO pageOperation = (CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO) operations
          .get(0);

      CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(restoredCacheEntry);
      Assert.assertEquals(4, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(2, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 3 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(3, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 4 }, nonLeafEntry.key);
      Assert.assertEquals(4, nonLeafEntry.leftChild);
      Assert.assertEquals(5, nonLeafEntry.rightChild);

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(1, restoredBucket.size());

      nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

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

      bucket.addNonLeafEntry(0, new byte[] { 1 }, 1, 2, true);

      final List<CellBTreeMultiValueV2Bucket.NonLeafEntry> nonLeafEntries = new ArrayList<>(3);

      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryOne = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 2 }, 2, 3);
      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryTwo = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 3 }, 3, 4);
      final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryThree = new CellBTreeMultiValueV2Bucket.NonLeafEntry(
          new byte[] { 4 }, 4, 5);

      nonLeafEntries.add(nonLeafEntryOne);
      nonLeafEntries.add(nonLeafEntryTwo);
      nonLeafEntries.add(nonLeafEntryThree);

      bucket.addAll(nonLeafEntries, OByteSerializer.INSTANCE, false);

      entry.clearPageOperations();

      bucket.shrink(1, OByteSerializer.INSTANCE, false);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO);

      final CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO pageOperation = (CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO) operations
          .get(0);

      final CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(entry);

      Assert.assertEquals(1, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

      pageOperation.undo(entry);

      Assert.assertEquals(4, restoredBucket.size());

      nonLeafEntry = restoredBucket.getNonLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, nonLeafEntry.key);
      Assert.assertEquals(1, nonLeafEntry.leftChild);
      Assert.assertEquals(2, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, nonLeafEntry.key);
      Assert.assertEquals(2, nonLeafEntry.leftChild);
      Assert.assertEquals(3, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 3 }, nonLeafEntry.key);
      Assert.assertEquals(3, nonLeafEntry.leftChild);
      Assert.assertEquals(4, nonLeafEntry.rightChild);

      nonLeafEntry = restoredBucket.getNonLeafEntry(3, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 4 }, nonLeafEntry.key);
      Assert.assertEquals(4, nonLeafEntry.leftChild);
      Assert.assertEquals(5, nonLeafEntry.rightChild);
      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final List<CellBTreeMultiValueV2Bucket.NonLeafEntry> nonLeafEntries = new ArrayList<>(3);

    final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryOne = new CellBTreeMultiValueV2Bucket.NonLeafEntry(new byte[] { 2 },
        1, 2);
    final CellBTreeMultiValueV2Bucket.NonLeafEntry nonLeafEntryTwo = new CellBTreeMultiValueV2Bucket.NonLeafEntry(new byte[] { 3 },
        2, 3);

    nonLeafEntries.add(nonLeafEntryOne);
    nonLeafEntries.add(nonLeafEntryTwo);

    CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO operation = new CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO(12,
        nonLeafEntries, OByteSerializer.INSTANCE, true);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO restoredOperation = new CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getNewSize());
    Assert.assertSame(OByteSerializer.INSTANCE, restoredOperation.getKeySerializer());
    Assert.assertTrue(restoredOperation.isEncrypted());

    final List<CellBTreeMultiValueV2Bucket.NonLeafEntry> restoredLeafEntries = restoredOperation.getRemovedEntries();
    Assert.assertEquals(2, restoredLeafEntries.size());

    final CellBTreeMultiValueV2Bucket.NonLeafEntry restoredNonLeafEntryOne = restoredLeafEntries.get(0);
    Assert.assertArrayEquals(new byte[] { 2 }, restoredNonLeafEntryOne.key);
    Assert.assertEquals(1, restoredNonLeafEntryOne.leftChild);
    Assert.assertEquals(2, restoredNonLeafEntryOne.rightChild);

    final CellBTreeMultiValueV2Bucket.NonLeafEntry restoredNonLeafEntryTwo = restoredLeafEntries.get(1);
    Assert.assertArrayEquals(new byte[] { 3 }, restoredNonLeafEntryTwo.key);
    Assert.assertEquals(2, restoredNonLeafEntryTwo.leftChild);
    Assert.assertEquals(3, restoredNonLeafEntryTwo.rightChild);
  }
}
