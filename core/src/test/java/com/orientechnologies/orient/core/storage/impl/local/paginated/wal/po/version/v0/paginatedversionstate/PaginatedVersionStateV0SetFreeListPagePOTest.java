package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.v0.paginatedversionstate;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.versionmap.OPaginatedVersionStateV0;
import com.orientechnologies.orient.core.storage.version.OVersionPage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class PaginatedVersionStateV0SetFreeListPagePOTest {
  @Test
  public void testRedo() {
    final int pageSize = OVersionPage.PAGE_SIZE;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OPaginatedVersionStateV0 versionState = new OPaginatedVersionStateV0(entry);
      versionState.setFreeListPage(0, 12);
      versionState.setFreeListPage(1, 14);
      versionState.setFreeListPage(2, 16);

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

      versionState.setFreeListPage(1, 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof PaginatedVersionStateV0SetFreeListPagePO);
      final PaginatedVersionStateV0SetFreeListPagePO pageOperation =
          (PaginatedVersionStateV0SetFreeListPagePO) operations.get(0);

      OPaginatedVersionStateV0 restoredPage = new OPaginatedVersionStateV0(restoredCacheEntry);

      Assert.assertEquals(12, restoredPage.getFreeListPage(0));
      Assert.assertEquals(14, restoredPage.getFreeListPage(1));
      Assert.assertEquals(16, restoredPage.getFreeListPage(2));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(12, restoredPage.getFreeListPage(0));
      Assert.assertEquals(42, restoredPage.getFreeListPage(1));
      Assert.assertEquals(16, restoredPage.getFreeListPage(2));

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndo() {
    final int pageSize = OVersionPage.PAGE_SIZE;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OPaginatedVersionStateV0 versionState = new OPaginatedVersionStateV0(entry);
      versionState.setFreeListPage(0, 12);
      versionState.setFreeListPage(1, 14);
      versionState.setFreeListPage(2, 16);

      entry.clearPageOperations();

      versionState.setFreeListPage(1, 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof PaginatedVersionStateV0SetFreeListPagePO);

      final PaginatedVersionStateV0SetFreeListPagePO pageOperation =
          (PaginatedVersionStateV0SetFreeListPagePO) operations.get(0);

      final OPaginatedVersionStateV0 restoredPage = new OPaginatedVersionStateV0(entry);

      Assert.assertEquals(12, restoredPage.getFreeListPage(0));
      Assert.assertEquals(42, restoredPage.getFreeListPage(1));
      Assert.assertEquals(16, restoredPage.getFreeListPage(2));

      pageOperation.undo(entry);

      Assert.assertEquals(12, restoredPage.getFreeListPage(0));
      Assert.assertEquals(14, restoredPage.getFreeListPage(1));
      Assert.assertEquals(16, restoredPage.getFreeListPage(2));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    PaginatedVersionStateV0SetFreeListPagePO operation =
        new PaginatedVersionStateV0SetFreeListPagePO(12, 42, 24);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    PaginatedVersionStateV0SetFreeListPagePO restoredOperation =
        new PaginatedVersionStateV0SetFreeListPagePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getIndex());
    Assert.assertEquals(42, restoredOperation.getOldPageIndex());
    Assert.assertEquals(24, restoredOperation.getNewPageIndex());
  }
}
