package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class LocalHashTableV2BucketSetDepthPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      HashIndexBucketV2<Byte, Byte> bucket = new HashIndexBucketV2<>(entry);
      bucket.init(2);

      bucket.addEntry(0, 1, new byte[] { (byte) 1 }, new byte[] { (byte) 2 });

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setDepth(3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2BucketSetDepthPO);

      final LocalHashTableV2BucketSetDepthPO pageOperation = (LocalHashTableV2BucketSetDepthPO) operations.get(0);

      HashIndexBucketV2<Byte, Byte> restoredBucket = new HashIndexBucketV2<>(restoredCacheEntry);

      Assert.assertEquals(2, restoredBucket.getDepth());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.getDepth());

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

      HashIndexBucketV2<Byte, Byte> bucket = new HashIndexBucketV2<>(entry);

      bucket.init(2);

      bucket.addEntry(0, 1, new byte[] { (byte) 1 }, new byte[] { (byte) 2 });

      entry.clearPageOperations();

      bucket.setDepth(3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2BucketSetDepthPO);

      final LocalHashTableV2BucketSetDepthPO pageOperation = (LocalHashTableV2BucketSetDepthPO) operations.get(0);

      final HashIndexBucketV2<Byte, Byte> restoredBucket = new HashIndexBucketV2<>(entry);

      Assert.assertEquals(3, restoredBucket.getDepth());

      pageOperation.undo(entry);

      Assert.assertEquals(2, restoredBucket.getDepth());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    LocalHashTableV2BucketSetDepthPO operation = new LocalHashTableV2BucketSetDepthPO((byte) 12, (byte) 34);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    LocalHashTableV2BucketSetDepthPO restoredOperation = new LocalHashTableV2BucketSetDepthPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getDepth());
    Assert.assertEquals(34, restoredOperation.getOldDepth());
  }
}
