package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeNullBucketV2;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeValue;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class SBTreeNullBucketV2SetValuePOTest {
  @Test
  public void testRedoNull() {
    final int pageSize = 64 * 1024;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      OSBTreeNullBucketV2<OIdentifiable> bucket = new OSBTreeNullBucketV2<>(entry);
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

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)), OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2SetValuePO);

      final SBTreeNullBucketV2SetValuePO pageOperation = (SBTreeNullBucketV2SetValuePO) operations.get(0);

      OSBTreeNullBucketV2<OIdentifiable> restoredBucket = new OSBTreeNullBucketV2<>(restoredCacheEntry);
      Assert.assertNull(restoredBucket.getValue(OLinkSerializer.INSTANCE));

      pageOperation.redo(restoredCacheEntry);

      final OSBTreeValue<OIdentifiable> btreeValue = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(btreeValue);

      Assert.assertEquals(new ORecordId(2, 2), btreeValue.getValue());

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

      OSBTreeNullBucketV2<OIdentifiable> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(1, 1)), OLinkSerializer.INSTANCE);
      entry.clearPageOperations();

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      final ByteBuffer originalBuffer = cachePointer.getBufferDuplicate();
      final ByteBuffer restoredBuffer = restoredCachePointer.getBufferDuplicate();

      Assert.assertNotNull(originalBuffer);
      Assert.assertNotNull(restoredBuffer);

      restoredBuffer.put(originalBuffer);

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)), OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2SetValuePO);

      final SBTreeNullBucketV2SetValuePO pageOperation = (SBTreeNullBucketV2SetValuePO) operations.get(0);

      OSBTreeNullBucketV2<OIdentifiable> restoredBucket = new OSBTreeNullBucketV2<>(restoredCacheEntry);
      OSBTreeValue value = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(value);
      Assert.assertEquals(new ORecordId(1, 1), value.getValue());

      pageOperation.redo(restoredCacheEntry);

      value = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(value);
      Assert.assertEquals(new ORecordId(2, 2), value.getValue());

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

      OSBTreeNullBucketV2<OIdentifiable> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      entry.clearPageOperations();

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)), OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2SetValuePO);

      final SBTreeNullBucketV2SetValuePO pageOperation = (SBTreeNullBucketV2SetValuePO) operations.get(0);

      final OSBTreeNullBucketV2<OIdentifiable> restoredBucket = new OSBTreeNullBucketV2<>(entry);

      OSBTreeValue<OIdentifiable> value = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(value);

      Assert.assertEquals(new ORecordId(2, 2), value.getValue());

      pageOperation.undo(entry);

      Assert.assertNull(restoredBucket.getValue(OLinkSerializer.INSTANCE));

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

      OSBTreeNullBucketV2<OIdentifiable> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(1, 1)), OLinkSerializer.INSTANCE);

      entry.clearPageOperations();

      bucket.setValue(OLinkSerializer.INSTANCE.serializeNativeAsWhole(new ORecordId(2, 2)), OLinkSerializer.INSTANCE);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2SetValuePO);

      final SBTreeNullBucketV2SetValuePO pageOperation = (SBTreeNullBucketV2SetValuePO) operations.get(0);

      final OSBTreeNullBucketV2<OIdentifiable> restoredBucket = new OSBTreeNullBucketV2<>(entry);

      OSBTreeValue<OIdentifiable> value = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(value);

      Assert.assertEquals(new ORecordId(2, 2), value.getValue());

      pageOperation.undo(entry);

      value = restoredBucket.getValue(OLinkSerializer.INSTANCE);
      Assert.assertNotNull(value);
      Assert.assertEquals(new ORecordId(1, 1), value.getValue());

      byteBufferPool.release(pointer);
    } finally {
      byteBufferPool.clear();
    }
  }

  @Test
  public void testSerializationNotNull() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    SBTreeNullBucketV2SetValuePO operation = new SBTreeNullBucketV2SetValuePO(new byte[] { 1, 2, 3, 4 }, new byte[] { 4, 3, 2, 1 },
        OLinkSerializer.INSTANCE);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    SBTreeNullBucketV2SetValuePO restoredOperation = new SBTreeNullBucketV2SetValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertArrayEquals(new byte[] { 1, 2, 3, 4 }, restoredOperation.getPrevValue());
    Assert.assertArrayEquals(new byte[] { 4, 3, 2, 1 }, restoredOperation.getValue());
    Assert.assertSame(OLinkSerializer.INSTANCE, restoredOperation.getValueSerializer());
  }

  @Test
  public void testSerializationNull() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    SBTreeNullBucketV2SetValuePO operation = new SBTreeNullBucketV2SetValuePO(null, new byte[] { 4, 3, 2, 1 },
        OLinkSerializer.INSTANCE);

    operation.setFileId(42);
    operation.setPageIndex(24);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int pos = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, pos);

    SBTreeNullBucketV2SetValuePO restoredOperation = new SBTreeNullBucketV2SetValuePO();
    restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(42, restoredOperation.getFileId());
    Assert.assertEquals(24, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    Assert.assertNull(restoredOperation.getPrevValue());
    Assert.assertArrayEquals(new byte[] { 4, 3, 2, 1 }, restoredOperation.getValue());
  }
}
