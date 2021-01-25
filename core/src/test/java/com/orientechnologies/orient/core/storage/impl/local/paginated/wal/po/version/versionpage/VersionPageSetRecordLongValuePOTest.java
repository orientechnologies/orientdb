package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.versionpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.version.OVersionPage;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class VersionPageSetRecordLongValuePOTest {
  @Test
  public void testRedo() {
    final int pageSize = OVersionPage.PAGE_SIZE;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OVersionPage versionPage = new OVersionPage(entry);
      versionPage.init();

      versionPage.appendRecord(
          1,
          new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
          -1,
          Collections.emptySet());
      versionPage.appendRecord(
          2,
          new byte[] {17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
          -1,
          Collections.emptySet());

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

      versionPage.setRecordLongValue(1, 2, 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageSetRecordLongValuePO);

      final VersionPageSetRecordLongValuePO pageOperation =
          (VersionPageSetRecordLongValuePO) operations.get(0);

      OVersionPage restoredPage = new OVersionPage(restoredCacheEntry);
      Assert.assertEquals(
          OLongSerializer.INSTANCE.deserializeNative(
              new byte[] {19, 20, 21, 22, 23, 24, 25, 26}, 0),
          restoredPage.getRecordLongValue(1, 2));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(42, restoredPage.getRecordLongValue(1, 2));

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
      final OPointer pointer = byteBufferPool.acquireDirect(false, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OVersionPage versionPage = new OVersionPage(entry);
      versionPage.init();

      versionPage.appendRecord(
          1,
          new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
          -1,
          Collections.emptySet());
      versionPage.appendRecord(
          2,
          new byte[] {17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
          -1,
          Collections.emptySet());

      entry.clearPageOperations();

      versionPage.setRecordLongValue(1, 2, 42);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageSetRecordLongValuePO);

      final VersionPageSetRecordLongValuePO pageOperation =
          (VersionPageSetRecordLongValuePO) operations.get(0);

      final OVersionPage restoredPage = new OVersionPage(entry);

      Assert.assertEquals(42, restoredPage.getRecordLongValue(1, 2));

      pageOperation.undo(entry);

      Assert.assertEquals(
          OLongSerializer.INSTANCE.deserializeNative(
              new byte[] {19, 20, 21, 22, 23, 24, 25, 26}, 0),
          restoredPage.getRecordLongValue(1, 2));
      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    VersionPageSetRecordLongValuePO operation =
        new VersionPageSetRecordLongValuePO(23, 12, 124, 421);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    VersionPageSetRecordLongValuePO restoredOperation = new VersionPageSetRecordLongValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(23, restoredOperation.getRecordPosition());
    Assert.assertEquals(12, restoredOperation.getOffset());
    Assert.assertEquals(124, restoredOperation.getValue());
    Assert.assertEquals(421, restoredOperation.getOldValue());
  }
}
