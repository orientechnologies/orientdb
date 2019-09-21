package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ClusterPageAppendRecordPOTest {
  @Test
  public void testRedo() {
    final int pageSize = OClusterPage.PAGE_SIZE;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OClusterPage clusterPage = new OClusterPage(entry);
      clusterPage.init();

      clusterPage.appendRecord(1, new byte[] { 1 }, -1, Collections.emptySet());
      clusterPage.appendRecord(2, new byte[] { 2 }, -1, Collections.emptySet());

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      clusterPage.appendRecord(3, new byte[] { 3 }, -1, Collections.emptySet());

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPageAppendRecordPO);

      final ClusterPageAppendRecordPO pageOperation = (ClusterPageAppendRecordPO) operations.get(0);

      OClusterPage restoredPage = new OClusterPage(restoredCacheEntry);
      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] { 1 }, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] { 2 }, restoredPage.getRecordBinaryValue(1, 0, 1));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(3, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));
      Assert.assertEquals(3, restoredPage.getRecordVersion(2));

      Assert.assertArrayEquals(new byte[] { 1 }, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] { 2 }, restoredPage.getRecordBinaryValue(1, 0, 1));
      Assert.assertArrayEquals(new byte[] { 3 }, restoredPage.getRecordBinaryValue(2, 0, 1));

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

      OClusterPage clusterPage = new OClusterPage(entry);
      clusterPage.init();

      clusterPage.appendRecord(1, new byte[] { 1 }, -1, Collections.emptySet());
      clusterPage.appendRecord(2, new byte[] { 2 }, -1, Collections.emptySet());

      entry.clearPageOperations();

      clusterPage.appendRecord(3, new byte[] { 3 }, -1, Collections.emptySet());

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPageAppendRecordPO);

      final ClusterPageAppendRecordPO pageOperation = (ClusterPageAppendRecordPO) operations.get(0);

      final OClusterPage restoredPage = new OClusterPage(entry);

      Assert.assertEquals(3, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));
      Assert.assertEquals(3, restoredPage.getRecordVersion(2));

      Assert.assertArrayEquals(new byte[] { 1 }, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] { 2 }, restoredPage.getRecordBinaryValue(1, 0, 1));
      Assert.assertArrayEquals(new byte[] { 3 }, restoredPage.getRecordBinaryValue(2, 0, 1));

      pageOperation.undo(entry);

      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] { 1 }, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] { 2 }, restoredPage.getRecordBinaryValue(1, 0, 1));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    ClusterPageAppendRecordPO operation = new ClusterPageAppendRecordPO(12, new byte[] { 4, 2 }, 23, 45, true);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    ClusterPageAppendRecordPO restoredOperation = new ClusterPageAppendRecordPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertTrue(restoredOperation.isAllocatedFromFreeList());
    Assert.assertEquals(12, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(new byte[] { 4, 2 }, restoredOperation.getRecord());
    Assert.assertEquals(23, restoredOperation.getRequestedPosition());
    Assert.assertEquals(45, restoredOperation.getRecordPosition());
  }

}
