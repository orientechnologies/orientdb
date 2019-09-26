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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CellBTreeMultiValueV2BucketAddAllLeafEntriesPOTest {
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

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      final List<CellBTreeMultiValueV2Bucket.LeafEntry> leafEntries = new ArrayList<>(3);

      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryOne = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 2 }, 2,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);
      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryTwo = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 3 }, 3,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);
      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryThree = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 4 }, 4,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);

      leafEntries.add(leafEntryOne);
      leafEntries.add(leafEntryTwo);
      leafEntries.add(leafEntryThree);

      bucket.addAll(leafEntries, OByteSerializer.INSTANCE, false);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketAddAllLeafEntriesPO);

      final CellBTreeMultiValueV2BucketAddAllLeafEntriesPO pageOperation = (CellBTreeMultiValueV2BucketAddAllLeafEntriesPO) operations
          .get(0);

      CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(restoredCacheEntry);
      Assert.assertEquals(1, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.LeafEntry leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, leafEntry.key);
      Assert.assertEquals(1, leafEntry.mId);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(1, leafEntry.values.size());
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(4, restoredBucket.size());

      leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, leafEntry.key);
      Assert.assertEquals(1, leafEntry.mId);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(1, leafEntry.values.size());
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, leafEntry.key);
      Assert.assertEquals(2, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 3 }, leafEntry.key);
      Assert.assertEquals(3, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

      leafEntry = restoredBucket.getLeafEntry(3, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 4 }, leafEntry.key);
      Assert.assertEquals(4, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

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

      entry.clearPageOperations();

      final List<CellBTreeMultiValueV2Bucket.LeafEntry> leafEntries = new ArrayList<>(3);

      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryOne = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 2 }, 2,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);
      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryTwo = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 3 }, 3,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);
      final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryThree = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 4 }, 4,
          Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);

      leafEntries.add(leafEntryOne);
      leafEntries.add(leafEntryTwo);
      leafEntries.add(leafEntryThree);

      bucket.addAll(leafEntries, OByteSerializer.INSTANCE, false);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2BucketAddAllLeafEntriesPO);

      final CellBTreeMultiValueV2BucketAddAllLeafEntriesPO pageOperation = (CellBTreeMultiValueV2BucketAddAllLeafEntriesPO) operations
          .get(0);

      final CellBTreeMultiValueV2Bucket<Byte> restoredBucket = new CellBTreeMultiValueV2Bucket<>(entry);

      Assert.assertEquals(4, restoredBucket.size());

      CellBTreeMultiValueV2Bucket.LeafEntry leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, leafEntry.key);
      Assert.assertEquals(1, leafEntry.mId);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(1, leafEntry.values.size());
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));

      leafEntry = restoredBucket.getLeafEntry(1, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 2 }, leafEntry.key);
      Assert.assertEquals(2, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

      leafEntry = restoredBucket.getLeafEntry(2, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 3 }, leafEntry.key);
      Assert.assertEquals(3, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

      leafEntry = restoredBucket.getLeafEntry(3, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 4 }, leafEntry.key);
      Assert.assertEquals(4, leafEntry.mId);
      Assert.assertEquals(4, leafEntry.entriesCount);
      Assert.assertEquals(2, leafEntry.values.size());

      Assert.assertEquals(new ORecordId(2, 2), leafEntry.values.get(0));
      Assert.assertEquals(new ORecordId(3, 3), leafEntry.values.get(1));

      pageOperation.undo(entry);

      Assert.assertEquals(1, restoredBucket.size());

      leafEntry = restoredBucket.getLeafEntry(0, OByteSerializer.INSTANCE, false);
      Assert.assertArrayEquals(new byte[] { 1 }, leafEntry.key);
      Assert.assertEquals(1, leafEntry.mId);
      Assert.assertEquals(1, leafEntry.entriesCount);
      Assert.assertEquals(1, leafEntry.values.size());
      Assert.assertEquals(new ORecordId(1, 1), leafEntry.values.get(0));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final List<CellBTreeMultiValueV2Bucket.LeafEntry> leafEntries = new ArrayList<>(3);

    final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryOne = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 2 }, 2,
        Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);
    final CellBTreeMultiValueV2Bucket.LeafEntry leafEntryTwo = new CellBTreeMultiValueV2Bucket.LeafEntry(new byte[] { 3 }, 3,
        Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), 4);

    leafEntries.add(leafEntryOne);
    leafEntries.add(leafEntryTwo);

    CellBTreeMultiValueV2BucketAddAllLeafEntriesPO operation = new CellBTreeMultiValueV2BucketAddAllLeafEntriesPO(3, leafEntries,
        OByteSerializer.INSTANCE, true);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2BucketAddAllLeafEntriesPO restoredOperation = new CellBTreeMultiValueV2BucketAddAllLeafEntriesPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(3, restoredOperation.getPrevSize());
    Assert.assertSame(OByteSerializer.INSTANCE, restoredOperation.getKeySerializer());
    Assert.assertTrue(restoredOperation.isEncrypted());

    final List<CellBTreeMultiValueV2Bucket.LeafEntry> restoredLeafEntries = restoredOperation.getLeafEntries();
    Assert.assertEquals(2, restoredLeafEntries.size());

    final CellBTreeMultiValueV2Bucket.LeafEntry restoredLeafEntryOne = restoredLeafEntries.get(0);
    Assert.assertArrayEquals(new byte[] { 2 }, restoredLeafEntryOne.key);
    Assert.assertEquals(2, restoredLeafEntryOne.mId);
    Assert.assertEquals(4, restoredLeafEntryOne.entriesCount);
    Assert.assertArrayEquals(new byte[] { 2 }, restoredLeafEntryOne.key);
    Assert.assertEquals(Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), restoredLeafEntryOne.values);

    final CellBTreeMultiValueV2Bucket.LeafEntry restoredLeafEntryTwo = restoredLeafEntries.get(1);
    Assert.assertArrayEquals(new byte[] { 3 }, restoredLeafEntryTwo.key);
    Assert.assertEquals(3, restoredLeafEntryTwo.mId);
    Assert.assertEquals(4, restoredLeafEntryTwo.entriesCount);
    Assert.assertArrayEquals(new byte[] { 3 }, restoredLeafEntryTwo.key);
    Assert.assertEquals(Arrays.asList(new ORecordId(2, 2), new ORecordId(3, 3)), restoredLeafEntryTwo.values);
  }
}
