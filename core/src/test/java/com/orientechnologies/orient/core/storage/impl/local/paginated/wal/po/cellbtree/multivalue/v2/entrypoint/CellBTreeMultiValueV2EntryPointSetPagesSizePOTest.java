package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2EntryPoint;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeMultiValueV2EntryPointSetPagesSizePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(entry);
      bucket.init();

      bucket.setPagesSize(42);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setPagesSize(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2EntryPointSetPagesSizePO);

      final CellBTreeMultiValueV2EntryPointSetPagesSizePO pageOperation = (CellBTreeMultiValueV2EntryPointSetPagesSizePO) operations
          .get(0);

      CellBTreeMultiValueV2EntryPoint restoredBucket = new CellBTreeMultiValueV2EntryPoint(restoredCacheEntry);

      Assert.assertEquals(42, restoredBucket.getPagesSize());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(24, restoredBucket.getPagesSize());

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

      CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(entry);
      bucket.init();

      bucket.setPagesSize(42);

      entry.clearPageOperations();

      bucket.setPagesSize(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2EntryPointSetPagesSizePO);
      final CellBTreeMultiValueV2EntryPointSetPagesSizePO pageOperation = (CellBTreeMultiValueV2EntryPointSetPagesSizePO) operations
          .get(0);

      final CellBTreeMultiValueV2EntryPoint restoredBucket = new CellBTreeMultiValueV2EntryPoint(entry);

      Assert.assertEquals(24, restoredBucket.getPagesSize());

      pageOperation.undo(entry);

      Assert.assertEquals(42, restoredBucket.getPagesSize());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeMultiValueV2EntryPointSetPagesSizePO operation = new CellBTreeMultiValueV2EntryPointSetPagesSizePO(12, 21);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2EntryPointSetPagesSizePO restoredOperation = new CellBTreeMultiValueV2EntryPointSetPagesSizePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getPagesSize());
    Assert.assertEquals(21, restoredOperation.getPrevPagesSize());
  }

}
