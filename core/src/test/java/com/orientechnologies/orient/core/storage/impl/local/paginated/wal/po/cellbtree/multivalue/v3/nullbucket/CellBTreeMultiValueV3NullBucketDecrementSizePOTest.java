package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3NullBucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeMultiValueV3NullBucketDecrementSizePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(entry);
      bucket.init(12);

      bucket.incrementSize();

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.decrementSize();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV3NullBucketDecrementSizePO);

      final CellBTreeMultiValueV3NullBucketDecrementSizePO pageOperation = (CellBTreeMultiValueV3NullBucketDecrementSizePO) operations
          .get(0);

      CellBTreeMultiValueV3NullBucket restoredBucket = new CellBTreeMultiValueV3NullBucket(restoredCacheEntry);

      Assert.assertEquals(1, restoredBucket.getSize());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(0, restoredBucket.getSize());

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

      CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(entry);
      bucket.init(12);

      bucket.incrementSize();

      entry.clearPageOperations();

      bucket.decrementSize();

      final List<PageOperationRecord> operations = entry.getPageOperations();

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV3NullBucketDecrementSizePO);
      final CellBTreeMultiValueV3NullBucketDecrementSizePO pageOperation = (CellBTreeMultiValueV3NullBucketDecrementSizePO) operations
          .get(0);

      final CellBTreeMultiValueV3NullBucket restoredBucket = new CellBTreeMultiValueV3NullBucket(entry);

      Assert.assertEquals(0, restoredBucket.getSize());

      pageOperation.undo(entry);

      Assert.assertEquals(1, restoredBucket.getSize());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }
}
