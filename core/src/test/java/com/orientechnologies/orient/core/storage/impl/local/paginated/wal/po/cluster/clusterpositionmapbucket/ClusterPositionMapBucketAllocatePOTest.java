package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ClusterPositionMapBucketAllocatePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 256;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(entry);
      bucket.init();

      bucket.add(12, 34);
      bucket.add(34, 56);

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

      bucket.allocate();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPositionMapBucketAllocatePO);

      final ClusterPositionMapBucketAllocatePO pageOperation =
          (ClusterPositionMapBucketAllocatePO) operations.get(0);

      OClusterPositionMapBucket restoredBucket = new OClusterPositionMapBucket(restoredCacheEntry);
      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredBucket.getSize());

      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));
      Assert.assertNull(restoredBucket.get(2));
      Assert.assertEquals(OClusterPositionMapBucket.ALLOCATED, restoredBucket.getStatus(2));

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndo() {
    final int pageSize = 256;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(entry);
      bucket.init();

      bucket.add(12, 34);
      bucket.add(34, 56);

      entry.clearPageOperations();

      bucket.allocate();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPositionMapBucketAllocatePO);

      final ClusterPositionMapBucketAllocatePO pageOperation =
          (ClusterPositionMapBucketAllocatePO) operations.get(0);

      OClusterPositionMapBucket restoredBucket = new OClusterPositionMapBucket(entry);
      Assert.assertEquals(3, restoredBucket.getSize());

      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));
      Assert.assertNull(restoredBucket.get(2));
      Assert.assertEquals(OClusterPositionMapBucket.ALLOCATED, restoredBucket.getStatus(2));

      pageOperation.undo(entry);

      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(
          new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }
}
