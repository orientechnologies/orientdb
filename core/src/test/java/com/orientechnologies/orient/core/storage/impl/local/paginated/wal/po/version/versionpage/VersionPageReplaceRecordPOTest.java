package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.versionpage;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
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

public class VersionPageReplaceRecordPOTest {
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

      versionPage.appendRecord(1, new byte[] {1}, -1, Collections.emptySet());
      versionPage.appendRecord(2, new byte[] {2}, -1, Collections.emptySet());

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

      Assert.assertEquals(2, versionPage.getRecordVersion(1));

      versionPage.replaceRecord(1, new byte[] {3}, 3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageReplaceRecordPO);

      final VersionPageReplaceRecordPO pageOperation =
          (VersionPageReplaceRecordPO) operations.get(0);

      OVersionPage restoredPage = new OVersionPage(restoredCacheEntry);
      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] {1}, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] {2}, restoredPage.getRecordBinaryValue(1, 0, 1));

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(3, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] {1}, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] {3}, restoredPage.getRecordBinaryValue(1, 0, 1));

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

      versionPage.appendRecord(1, new byte[] {1}, -1, Collections.emptySet());
      versionPage.appendRecord(2, new byte[] {2}, -1, Collections.emptySet());

      entry.clearPageOperations();

      versionPage.replaceRecord(1, new byte[] {3}, 3);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof VersionPageReplaceRecordPO);

      final VersionPageReplaceRecordPO pageOperation =
          (VersionPageReplaceRecordPO) operations.get(0);

      final OVersionPage restoredPage = new OVersionPage(entry);

      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(3, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] {1}, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] {3}, restoredPage.getRecordBinaryValue(1, 0, 1));

      pageOperation.undo(entry);

      Assert.assertEquals(2, restoredPage.getRecordsCount());

      Assert.assertEquals(1, restoredPage.getRecordVersion(0));
      Assert.assertEquals(2, restoredPage.getRecordVersion(1));

      Assert.assertArrayEquals(new byte[] {1}, restoredPage.getRecordBinaryValue(0, 0, 1));
      Assert.assertArrayEquals(new byte[] {2}, restoredPage.getRecordBinaryValue(1, 0, 1));

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerialization() {
    VersionPageReplaceRecordPO operation =
        new VersionPageReplaceRecordPO(23, 12, new byte[] {4, 2}, 21, new byte[] {2, 4});

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    VersionPageReplaceRecordPO restoredOperation = new VersionPageReplaceRecordPO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(23, restoredOperation.getRecordPosition());
    Assert.assertEquals(12, restoredOperation.getRecordVersion());
    Assert.assertArrayEquals(new byte[] {4, 2}, restoredOperation.getRecord());
    Assert.assertEquals(21, restoredOperation.getOldRecordVersion());
    Assert.assertArrayEquals(new byte[] {2, 4}, restoredOperation.getOldRecord());
  }
}
