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

public class CellBTreeMultiValueV2EntryPointSetEntryIdPOTest {
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

      bucket.setEntryId(42);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setEntryId(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2EntryPointSetEntryIdPO);

      final CellBTreeMultiValueV2EntryPointSetEntryIdPO pageOperation = (CellBTreeMultiValueV2EntryPointSetEntryIdPO) operations
          .get(0);

      CellBTreeMultiValueV2EntryPoint restoredBucket = new CellBTreeMultiValueV2EntryPoint(restoredCacheEntry);

      Assert.assertEquals(42, restoredBucket.getEntryId());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(24, restoredBucket.getEntryId());

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

      bucket.setEntryId(42);

      entry.clearPageOperations();

      bucket.setEntryId(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();

      Assert.assertTrue(operations.get(0) instanceof CellBTreeMultiValueV2EntryPointSetEntryIdPO);
      final CellBTreeMultiValueV2EntryPointSetEntryIdPO pageOperation = (CellBTreeMultiValueV2EntryPointSetEntryIdPO) operations
          .get(0);

      final CellBTreeMultiValueV2EntryPoint restoredBucket = new CellBTreeMultiValueV2EntryPoint(entry);

      Assert.assertEquals(24, restoredBucket.getEntryId());

      pageOperation.undo(entry);

      Assert.assertEquals(42, restoredBucket.getEntryId());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeMultiValueV2EntryPointSetEntryIdPO operation = new CellBTreeMultiValueV2EntryPointSetEntryIdPO(12, 21);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeMultiValueV2EntryPointSetEntryIdPO restoredOperation = new CellBTreeMultiValueV2EntryPointSetEntryIdPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(12, restoredOperation.getEntryId());
    Assert.assertEquals(21, restoredOperation.getPrevEntryId());
  }

}
