package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.versionpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.version.OVersionPage;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class VersionPageSetNextPagePOTest {
  @Test
  public void testRedo() {
    final int pageSize = OVersionPage.PAGE_SIZE;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OVersionPage versionPage = new OVersionPage(entry);
      versionPage.init();

      versionPage.setNextPage(20);

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

      versionPage.setNextPage(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageSetNextPagePO);
      final VersionPageSetNextPagePO pageOperation = (VersionPageSetNextPagePO) operations.get(0);

      OVersionPage restoredPage = new OVersionPage(restoredCacheEntry);
      Assert.assertEquals(20, restoredPage.getNextPage());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(42, restoredPage.getNextPage());

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

      OVersionPage versionPage = new OVersionPage(entry);
      versionPage.init();

      versionPage.setNextPage(30);

      entry.clearPageOperations();

      versionPage.setNextPage(42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageSetNextPagePO);

      final VersionPageSetNextPagePO pageOperation = (VersionPageSetNextPagePO) operations.get(0);

      final OVersionPage restoredPage = new OVersionPage(entry);

      Assert.assertEquals(42, restoredPage.getNextPage());

      pageOperation.undo(entry);

      Assert.assertEquals(30, restoredPage.getNextPage());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    VersionPageSetNextPagePO operation = new VersionPageSetNextPagePO(12, 45);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    VersionPageSetNextPagePO restoredOperation = new VersionPageSetNextPagePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getNextPage());
    Assert.assertEquals(45, restoredOperation.getOldNextPage());
  }
}
