package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueEntryPointV3;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CellBTreeSingleValueEntryPointV3SetFreeListHeadPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      CellBTreeSingleValueEntryPointV3<?> bucket = new CellBTreeSingleValueEntryPointV3<>(entry);
      bucket.init();

      bucket.setFreeListHead(42);

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer, false);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setFreeListHead(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(
          operations.get(0) instanceof CellBTreeSingleValueEntryPointV3SetFreeListHeadPO);

      final CellBTreeSingleValueEntryPointV3SetFreeListHeadPO pageOperation =
          (CellBTreeSingleValueEntryPointV3SetFreeListHeadPO) operations.get(0);

      CellBTreeSingleValueEntryPointV3<?> restoredBucket =
          new CellBTreeSingleValueEntryPointV3<>(restoredCacheEntry);
      Assert.assertEquals(42, restoredBucket.getFreeListHead());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(24, restoredBucket.getFreeListHead());

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
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      CellBTreeSingleValueEntryPointV3<?> bucket = new CellBTreeSingleValueEntryPointV3<>(entry);
      bucket.init();

      bucket.setFreeListHead(42);

      entry.clearPageOperations();

      bucket.setFreeListHead(24);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(
          operations.get(0) instanceof CellBTreeSingleValueEntryPointV3SetFreeListHeadPO);

      final CellBTreeSingleValueEntryPointV3SetFreeListHeadPO pageOperation =
          (CellBTreeSingleValueEntryPointV3SetFreeListHeadPO) operations.get(0);

      final CellBTreeSingleValueEntryPointV3<?> restoredBucket =
          new CellBTreeSingleValueEntryPointV3<>(entry);

      Assert.assertEquals(24, restoredBucket.getFreeListHead());

      pageOperation.undo(entry);

      Assert.assertEquals(42, restoredBucket.getFreeListHead());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    CellBTreeSingleValueEntryPointV3SetFreeListHeadPO operation =
        new CellBTreeSingleValueEntryPointV3SetFreeListHeadPO(42, 24);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeSingleValueEntryPointV3SetFreeListHeadPO restoredOperation =
        new CellBTreeSingleValueEntryPointV3SetFreeListHeadPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(42, restoredOperation.getFreeListHead());
    Assert.assertEquals(24, restoredOperation.getPrevFreeListHead());
  }
}
