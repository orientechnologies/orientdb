package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage.LocalHashTableV2DirectoryPageSetMaxLeftChildDepthPO;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryFirstPageV2;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      DirectoryFirstPageV2 page = new DirectoryFirstPageV2(entry);
      page.setMaxLeftChildDepth(2, (byte) 24);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      page.setMaxLeftChildDepth(2, (byte) 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO);

      final LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO pageOperation = (LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO) operations
          .get(0);

      DirectoryFirstPageV2 restoredPage = new DirectoryFirstPageV2(restoredCacheEntry);
      Assert.assertEquals(24, restoredPage.getMaxLeftChildDepth(2));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(42, restoredPage.getMaxLeftChildDepth(2));

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

      DirectoryFirstPageV2 page = new DirectoryFirstPageV2(entry);
      page.setMaxLeftChildDepth(2, (byte) 24);

      entry.clearPageOperations();

      page.setMaxLeftChildDepth(2, (byte) 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO);

      final LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO pageOperation = (LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO) operations
          .get(0);

      final DirectoryFirstPageV2 restoredPage = new DirectoryFirstPageV2(entry);

      Assert.assertEquals(42, restoredPage.getMaxLeftChildDepth(2));

      pageOperation.undo(entry);

      Assert.assertEquals(24, restoredPage.getMaxLeftChildDepth(2));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO operation = new LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO(
        2, (byte) 12, (byte) 21);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    LocalHashTableV2DirectoryPageSetMaxLeftChildDepthPO restoredOperation = new LocalHashTableV2DirectoryPageSetMaxLeftChildDepthPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(2, restoredOperation.getLocalNodeIndex());
    Assert.assertEquals(12, restoredOperation.getMaxLeftChildDepth());
    Assert.assertEquals(21, restoredOperation.getPastMaxLeftChildDepth());
  }
}
