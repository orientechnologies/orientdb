package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexNullBucketV2;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class LocalHashTableV2NullBucketRemoveValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      HashIndexNullBucketV2<Byte> bucket = new HashIndexNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(new byte[] { (byte) 2 }, null);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.removeValue(new byte[] { (byte) 2 });

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2NullBucketRemoveValuePO);

      final LocalHashTableV2NullBucketRemoveValuePO pageOperation = (LocalHashTableV2NullBucketRemoveValuePO) operations.get(0);

      HashIndexNullBucketV2<Byte> restoredBucket = new HashIndexNullBucketV2<>(restoredCacheEntry);

      Assert.assertEquals(Byte.valueOf((byte) 2), restoredBucket.getValue(OByteSerializer.INSTANCE));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertNull(restoredBucket.getValue(OByteSerializer.INSTANCE));

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

      HashIndexNullBucketV2<Byte> bucket = new HashIndexNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(new byte[] { (byte) 2 }, null);

      entry.clearPageOperations();

      bucket.removeValue(new byte[] { (byte) 2 });

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2NullBucketRemoveValuePO);

      final LocalHashTableV2NullBucketRemoveValuePO pageOperation = (LocalHashTableV2NullBucketRemoveValuePO) operations.get(0);

      final HashIndexNullBucketV2<Byte> restoredBucket = new HashIndexNullBucketV2<>(entry);

      Assert.assertNull(restoredBucket.getValue(OByteSerializer.INSTANCE));

      pageOperation.undo(entry);

      Assert.assertEquals(Byte.valueOf((byte) 2), restoredBucket.getValue(OByteSerializer.INSTANCE));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    LocalHashTableV2NullBucketRemoveValuePO operation = new LocalHashTableV2NullBucketRemoveValuePO(new byte[] { (byte) 2 });

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    LocalHashTableV2NullBucketRemoveValuePO restoredOperation = new LocalHashTableV2NullBucketRemoveValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertArrayEquals(new byte[] { (byte) 2 }, restoredOperation.getPrevValue());
  }
}
