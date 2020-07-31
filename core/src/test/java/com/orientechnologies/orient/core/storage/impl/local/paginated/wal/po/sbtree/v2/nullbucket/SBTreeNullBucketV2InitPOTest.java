package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeNullBucketV2;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SBTreeNullBucketV2InitPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 256;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer, false);

      OSBTreeNullBucketV2<Byte> bucket = new OSBTreeNullBucketV2<>(entry);
      bucket.init();

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof SBTreeNullBucketV2InitPO);

      final SBTreeNullBucketV2InitPO pageOperation = (SBTreeNullBucketV2InitPO) operations.get(0);

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer =
          new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer, false);

      pageOperation.redo(restoredCacheEntry);

      OSBTreeNullBucketV2<Byte> restoredPage = new OSBTreeNullBucketV2<>(restoredCacheEntry);

      Assert.assertNull(restoredPage.getValue(OByteSerializer.INSTANCE));

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }
}
