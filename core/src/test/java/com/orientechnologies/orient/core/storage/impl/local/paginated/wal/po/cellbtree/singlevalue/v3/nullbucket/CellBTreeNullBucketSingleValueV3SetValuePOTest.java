package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3NullBucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CellBTreeNullBucketSingleValueV3SetValuePOTest {
  @Test
  public void testRedoNull() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setValue(new ORecordId(2, 2));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3SetValuePO);

      final CellBTreeNullBucketSingleValueV3SetValuePO pageOperation = (CellBTreeNullBucketSingleValueV3SetValuePO) operations
          .get(0);

      CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(restoredCacheEntry);
      Assert.assertNull(restoredBucket.getValue());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testRedoNotNull() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      bucket.setValue(new ORecordId(1, 1));
      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setValue(new ORecordId(2, 2));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3SetValuePO);

      final CellBTreeNullBucketSingleValueV3SetValuePO pageOperation = (CellBTreeNullBucketSingleValueV3SetValuePO) operations
          .get(0);

      CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(restoredCacheEntry);
      Assert.assertEquals(new ORecordId(1, 1), restoredBucket.getValue());

      pageOperation.redo(restoredCacheEntry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndoNull() {
    final int pageSize = 64 * 1024;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      entry.clearPageOperations();

      bucket.setValue(new ORecordId(2, 2));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3SetValuePO);

      final CellBTreeNullBucketSingleValueV3SetValuePO pageOperation = (CellBTreeNullBucketSingleValueV3SetValuePO) operations
          .get(0);

      final CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(entry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      pageOperation.undo(entry);

      Assert.assertNull(restoredBucket.getValue());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testUndoNotNull() {
    final int pageSize = 64 * 1024;

    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(entry);
      bucket.init();

      bucket.setValue(new ORecordId(1, 1));

      entry.clearPageOperations();

      bucket.setValue(new ORecordId(2, 2));

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof CellBTreeNullBucketSingleValueV3SetValuePO);

      final CellBTreeNullBucketSingleValueV3SetValuePO pageOperation = (CellBTreeNullBucketSingleValueV3SetValuePO) operations
          .get(0);

      final CellBTreeSingleValueV3NullBucket restoredBucket = new CellBTreeSingleValueV3NullBucket(entry);

      Assert.assertEquals(new ORecordId(2, 2), restoredBucket.getValue());

      pageOperation.undo(entry);

      Assert.assertEquals(new ORecordId(1, 1), restoredBucket.getValue());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerializationNotNull() {
    CellBTreeNullBucketSingleValueV3SetValuePO operation = new CellBTreeNullBucketSingleValueV3SetValuePO(new ORecordId(1, 1),
        new ORecordId(2, 2));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeNullBucketSingleValueV3SetValuePO restoredOperation = new CellBTreeNullBucketSingleValueV3SetValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertEquals(new ORecordId(1, 1), restoredOperation.getPrevValue());
    Assert.assertEquals(new ORecordId(2, 2), restoredOperation.getValue());
  }

  @Test
  public void testSerializationNull() {
    CellBTreeNullBucketSingleValueV3SetValuePO operation = new CellBTreeNullBucketSingleValueV3SetValuePO(null,
        new ORecordId(2, 2));

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(1);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    CellBTreeNullBucketSingleValueV3SetValuePO restoredOperation = new CellBTreeNullBucketSingleValueV3SetValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(1, restoredOperation.getOperationUnitId());

    Assert.assertNull(restoredOperation.getPrevValue());
    Assert.assertEquals(new ORecordId(2, 2), restoredOperation.getValue());
  }
}
