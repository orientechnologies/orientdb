package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class LocalHashTableV2BucketAddEntryPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      HashIndexBucketV2<Byte, Byte> bucket = new HashIndexBucketV2<>(entry);
      bucket.init(2);

      bucket.addEntry(0, 1, new byte[] {(byte) 1}, new byte[] {(byte) 2});

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

      bucket.addEntry(1, 2, new byte[] {(byte) 2}, new byte[] {(byte) 3});

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2BucketAddEntryPO);

      final LocalHashTableV2BucketAddEntryPO pageOperation =
          (LocalHashTableV2BucketAddEntryPO) operations.get(0);

      HashIndexBucketV2<Byte, Byte> restoredBucket = new HashIndexBucketV2<>(restoredCacheEntry);
      Assert.assertEquals(1, restoredBucket.size());

      OHashTable.Entry<Byte, Byte> bucketEntry =
          restoredBucket.getEntry(0, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 1), bucketEntry.key);
      Assert.assertEquals(1, bucketEntry.hashCode);

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(2, restoredBucket.size());

      bucketEntry =
          restoredBucket.getEntry(0, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 1), bucketEntry.key);
      Assert.assertEquals(1, bucketEntry.hashCode);

      bucketEntry =
          restoredBucket.getEntry(1, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 3), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.key);
      Assert.assertEquals(2, bucketEntry.hashCode);

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

      HashIndexBucketV2<Byte, Byte> bucket = new HashIndexBucketV2<>(entry);

      bucket.init(2);

      bucket.addEntry(0, 1, new byte[] {(byte) 1}, new byte[] {(byte) 2});

      entry.clearPageOperations();

      bucket.addEntry(1, 2, new byte[] {(byte) 2}, new byte[] {(byte) 3});

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2BucketAddEntryPO);

      final LocalHashTableV2BucketAddEntryPO pageOperation =
          (LocalHashTableV2BucketAddEntryPO) operations.get(0);

      final HashIndexBucketV2<Byte, Byte> restoredBucket = new HashIndexBucketV2<>(entry);

      OHashTable.Entry<Byte, Byte> bucketEntry =
          restoredBucket.getEntry(0, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 1), bucketEntry.key);
      Assert.assertEquals(1, bucketEntry.hashCode);

      bucketEntry =
          restoredBucket.getEntry(1, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 3), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.key);
      Assert.assertEquals(2, bucketEntry.hashCode);

      pageOperation.undo(entry);

      bucketEntry =
          restoredBucket.getEntry(0, null, OByteSerializer.INSTANCE, OByteSerializer.INSTANCE);
      Assert.assertEquals(Byte.valueOf((byte) 2), bucketEntry.value);
      Assert.assertEquals(Byte.valueOf((byte) 1), bucketEntry.key);
      Assert.assertEquals(1, bucketEntry.hashCode);

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    LocalHashTableV2BucketAddEntryPO operation =
        new LocalHashTableV2BucketAddEntryPO(1, 24, new byte[] {1, 2}, new byte[] {3, 4});

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    LocalHashTableV2BucketAddEntryPO restoredOperation = new LocalHashTableV2BucketAddEntryPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(1, restoredOperation.getIndex());
    Assert.assertArrayEquals(new byte[] {1, 2}, restoredOperation.getKey());
    Assert.assertArrayEquals(new byte[] {3, 4}, restoredOperation.getValue());
    Assert.assertEquals(24, restoredOperation.getHashCode());
  }
}
