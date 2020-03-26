package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterStateV0;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class PaginatedClusterStateV0SetRecordsSizePOTest {
  @Test
  public void testRedo() {
    final int pageSize = OClusterPage.PAGE_SIZE;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OPaginatedClusterStateV0 clusterState = new OPaginatedClusterStateV0(entry);
      clusterState.setRecordsSize(12);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      clusterState.setRecordsSize(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof PaginatedClusterStateV0SetRecordsSizePO);
      final PaginatedClusterStateV0SetRecordsSizePO pageOperation = (PaginatedClusterStateV0SetRecordsSizePO) operations.get(0);

      OPaginatedClusterStateV0 restoredPage = new OPaginatedClusterStateV0(restoredCacheEntry);
      Assert.assertEquals(12, restoredPage.getRecordsSize());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(42, restoredPage.getRecordsSize());

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndo() {
    final int pageSize = OClusterPage.PAGE_SIZE;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OPaginatedClusterStateV0 clusterState = new OPaginatedClusterStateV0(entry);
      clusterState.setRecordsSize(12);

      entry.clearPageOperations();

      clusterState.setRecordsSize(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof PaginatedClusterStateV0SetRecordsSizePO);

      final PaginatedClusterStateV0SetRecordsSizePO pageOperation = (PaginatedClusterStateV0SetRecordsSizePO) operations.get(0);

      final OPaginatedClusterStateV0 restoredPage = new OPaginatedClusterStateV0(entry);

      Assert.assertEquals(42, restoredPage.getRecordsSize());

      pageOperation.undo(entry);

      Assert.assertEquals(12, restoredPage.getRecordsSize());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    PaginatedClusterStateV0SetRecordsSizePO operation = new PaginatedClusterStateV0SetRecordsSizePO(12, 42);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    PaginatedClusterStateV0SetRecordsSizePO restoredOperation = new PaginatedClusterStateV0SetRecordsSizePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getOldRecordsSize());
    Assert.assertEquals(42, restoredOperation.getNewRecordsSize());
  }

}
