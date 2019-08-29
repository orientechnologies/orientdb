package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueEntryPointV3;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeEntryPointSingleValueV3SetTreeSizePOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueEntryPointV3 bucket = new CellBTreeSingleValueEntryPointV3(entry);
      bucket.init();

      bucket.setTreeSize(42);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setTreeSize(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeEntryPointSingleValueV3SetTreeSizePO);

      final CellBTreeEntryPointSingleValueV3SetTreeSizePO pageOperation = (CellBTreeEntryPointSingleValueV3SetTreeSizePO) operations
          .get(0);

      CellBTreeSingleValueEntryPointV3 restoredBucket = new CellBTreeSingleValueEntryPointV3(restoredCacheEntry);
      Assert.assertEquals(42, restoredBucket.getTreeSize());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(24, restoredBucket.getTreeSize());

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

      CellBTreeSingleValueEntryPointV3 bucket = new CellBTreeSingleValueEntryPointV3(entry);
      bucket.init();

      bucket.setTreeSize(42);

      entry.clearPageOperations();

      bucket.setTreeSize(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeEntryPointSingleValueV3SetTreeSizePO);

      final CellBTreeEntryPointSingleValueV3SetTreeSizePO pageOperation = (CellBTreeEntryPointSingleValueV3SetTreeSizePO) operations
          .get(0);

      final CellBTreeSingleValueEntryPointV3 restoredBucket = new CellBTreeSingleValueEntryPointV3(entry);

      Assert.assertEquals(24, restoredBucket.getTreeSize());

      pageOperation.undo(entry);

      Assert.assertEquals(42, restoredBucket.getTreeSize());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    CellBTreeEntryPointSingleValueV3SetTreeSizePO operation = new CellBTreeEntryPointSingleValueV3SetTreeSizePO(42, 24);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeEntryPointSingleValueV3SetTreeSizePO restoredOperation = new CellBTreeEntryPointSingleValueV3SetTreeSizePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertEquals(42, restoredOperation.getPrevTreeSize());
    Assert.assertEquals(24, restoredOperation.getTreeSize());
  }

}
