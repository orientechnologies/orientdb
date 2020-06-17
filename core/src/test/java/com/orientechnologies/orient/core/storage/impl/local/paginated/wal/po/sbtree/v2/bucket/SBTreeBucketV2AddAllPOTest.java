package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeBucketV2;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SBTreeBucketV2AddAllPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OSBTreeBucketV2<Byte, OIdentifiable> bucket = new OSBTreeBucketV2<>(entry);
      bucket.init(true);

      bucket.addLeafEntry(
          0, new byte[] {0}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(0, 0)));
      bucket.addLeafEntry(
          1, new byte[] {1}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(1, 1)));
      bucket.addLeafEntry(
          2, new byte[] {2}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)));

      final List<byte[]> rawEntries = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        rawEntries.add(bucket.getRawEntry(i, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE));
      }
      bucket.shrink(0, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.addAll(rawEntries, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeBucketV2AddAllPO);

      final SBTreeBucketV2AddAllPO pageOperation = (SBTreeBucketV2AddAllPO) operations.get(0);

      OSBTreeBucketV2<Byte, OIdentifiable> restoredBucket =
          new OSBTreeBucketV2<>(restoredCacheEntry);
      Assert.assertEquals(0, restoredBucket.size());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0),
          restoredBucket
              .getValue(0, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());
      Assert.assertEquals(
          new ORecordId(1, 1),
          restoredBucket
              .getValue(1, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());
      Assert.assertEquals(
          new ORecordId(2, 2),
          restoredBucket
              .getValue(2, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());

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

      OSBTreeBucketV2<Byte, OIdentifiable> bucket = new OSBTreeBucketV2<>(entry);
      bucket.init(true);

      bucket.addLeafEntry(
          0, new byte[] {0}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(0, 0)));
      bucket.addLeafEntry(
          1, new byte[] {1}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(1, 1)));
      bucket.addLeafEntry(
          2, new byte[] {2}, OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)));

      final List<byte[]> rawEntries = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        rawEntries.add(bucket.getRawEntry(i, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE));
      }

      bucket.shrink(0, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      entry.clearPageOperations();

      bucket.addAll(rawEntries, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeBucketV2AddAllPO);

      final SBTreeBucketV2AddAllPO pageOperation = (SBTreeBucketV2AddAllPO) operations.get(0);

      final OSBTreeBucketV2<Byte, OIdentifiable> restoredBucket = new OSBTreeBucketV2<>(entry);

      Assert.assertEquals(3, restoredBucket.size());

      Assert.assertEquals(
          new ORecordId(0, 0),
          restoredBucket
              .getValue(0, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());
      Assert.assertEquals(
          new ORecordId(1, 1),
          restoredBucket
              .getValue(1, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());
      Assert.assertEquals(
          new ORecordId(2, 2),
          restoredBucket
              .getValue(2, OByteSerializer.INSTANCE, OLinkSerializer.INSTANCE)
              .getValue());

      pageOperation.undo(entry);

      Assert.assertEquals(0, restoredBucket.size());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    final List<byte[]> rawRecords = new ArrayList<>();
    rawRecords.add(new byte[] {4, 2});
    rawRecords.add(new byte[] {2, 4});

    SBTreeBucketV2AddAllPO operation =
        new SBTreeBucketV2AddAllPO(
            3, rawRecords, OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    SBTreeBucketV2AddAllPO restoredOperation = new SBTreeBucketV2AddAllPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(2, restoredOperation.getRawRecords().size());
    for (int i = 0; i < 2; i++) {
      final byte[] record = rawRecords.get(i);
      final byte[] storedRecord = restoredOperation.getRawRecords().get(i);

      Assert.assertArrayEquals(record, storedRecord);
    }

    Assert.assertSame(OIntegerSerializer.INSTANCE, restoredOperation.getKeySerializer());
    Assert.assertSame(OLinkSerializer.INSTANCE, restoredOperation.getValueSerializer());
    Assert.assertEquals(3, restoredOperation.getPrevSize());
  }
}
