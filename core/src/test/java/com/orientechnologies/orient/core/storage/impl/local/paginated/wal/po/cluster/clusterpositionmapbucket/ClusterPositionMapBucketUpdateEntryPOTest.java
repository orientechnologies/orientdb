package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class ClusterPositionMapBucketUpdateEntryPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 256;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(entry);
      bucket.init();

      bucket.add(12, 34);
      bucket.add(34, 56);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.set(1, new OClusterPositionMapBucket.PositionEntry(24, 42));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPositionMapBucketUpdateEntryPO);

      final ClusterPositionMapBucketUpdateEntryPO pageOperation = (ClusterPositionMapBucketUpdateEntryPO) operations.get(0);

      OClusterPositionMapBucket restoredBucket = new OClusterPositionMapBucket(restoredCacheEntry);
      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(24, 42), restoredBucket.get(1));

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
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(entry);
      bucket.init();

      bucket.add(12, 34);
      bucket.add(34, 56);

      entry.clearPageOperations();

      bucket.set(1, new OClusterPositionMapBucket.PositionEntry(24, 42));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPositionMapBucketUpdateEntryPO);

      final ClusterPositionMapBucketUpdateEntryPO pageOperation = (ClusterPositionMapBucketUpdateEntryPO) operations.get(0);

      OClusterPositionMapBucket restoredBucket = new OClusterPositionMapBucket(entry);

      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(24, 42), restoredBucket.get(1));

      pageOperation.undo(entry);


      Assert.assertEquals(2, restoredBucket.getSize());

      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(12, 34), restoredBucket.get(0));
      Assert.assertEquals(new OClusterPositionMapBucket.PositionEntry(34, 56), restoredBucket.get(1));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    ClusterPositionMapBucketUpdateEntryPO operation = new ClusterPositionMapBucketUpdateEntryPO(23,
        OClusterPositionMapBucket.FILLED, 45, 43, OClusterPositionMapBucket.ALLOCATED, 90, 1);
    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    ClusterPositionMapBucketUpdateEntryPO restoredOperation = new ClusterPositionMapBucketUpdateEntryPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(23, restoredOperation.getIndex());
    Assert.assertEquals(OClusterPositionMapBucket.FILLED, restoredOperation.getOldRecordStatus());
    Assert.assertEquals(45, restoredOperation.getOldRecordPageIndex());
    Assert.assertEquals(43, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(OClusterPositionMapBucket.ALLOCATED, restoredOperation.getRecordStatus());
    Assert.assertEquals(90, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(1, restoredOperation.getRecordPosition());
  }
}
