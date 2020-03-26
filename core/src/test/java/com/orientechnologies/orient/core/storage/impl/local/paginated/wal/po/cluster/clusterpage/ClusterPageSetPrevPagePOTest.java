package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class ClusterPageSetPrevPagePOTest {
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

      clusterPage.setPrevPage(20);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      clusterPage.setPrevPage(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPageSetPrevPagePO);
      final ClusterPageSetPrevPagePO pageOperation = (ClusterPageSetPrevPagePO) operations.get(0);

      OClusterPage restoredPage = new OClusterPage(restoredCacheEntry);
      Assert.assertEquals(20, restoredPage.getPrevPage());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(42, restoredPage.getPrevPage());

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

      clusterPage.setPrevPage(30);

      entry.clearPageOperations();

      clusterPage.setPrevPage(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof ClusterPageSetPrevPagePO);

      final ClusterPageSetPrevPagePO pageOperation = (ClusterPageSetPrevPagePO) operations.get(0);

      final OClusterPage restoredPage = new OClusterPage(entry);

      Assert.assertEquals(42, restoredPage.getPrevPage());

      pageOperation.undo(entry);

      Assert.assertEquals(30, restoredPage.getPrevPage());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    ClusterPageSetPrevPagePO operation = new ClusterPageSetPrevPagePO(12, 45);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    ClusterPageSetPrevPagePO restoredOperation = new ClusterPageSetPrevPagePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getOldPrevPage());
    Assert.assertEquals(45, restoredOperation.getPrevPage());
  }
}
