package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexBucketV2;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LocalHashTableV2BucketInitPOTest {
  @Test
  public void testRedo() {
    final int pageSize = 256;
    final OByteBufferPool byteBufferPool = new OByteBufferPool(pageSize);
    try {
      final OPointer pointer = byteBufferPool.acquireDirect(false);
      final OCachePointer cachePointer = new OCachePointer(pointer, byteBufferPool, 0, 0);
      final OCacheEntry entry = new OCacheEntryImpl(0, 0, cachePointer);

      HashIndexBucketV2 bucket = new HashIndexBucketV2(entry);
      bucket.init(1);

      final List<PageOperationRecord> operations = entry.getPageOperations();
      Assert.assertEquals(1, operations.size());

      Assert.assertTrue(operations.get(0) instanceof LocalHashTableV2BucketInitPO);

      final LocalHashTableV2BucketInitPO pageOperation = (LocalHashTableV2BucketInitPO) operations.get(0);

      final OPointer restoredPointer = byteBufferPool.acquireDirect(false);
      final OCachePointer restoredCachePointer = new OCachePointer(restoredPointer, byteBufferPool, 0, 0);
      final OCacheEntry restoredCacheEntry = new OCacheEntryImpl(0, 0, restoredCachePointer);

      pageOperation.redo(restoredCacheEntry);

      HashIndexBucketV2 restoredPage = new HashIndexBucketV2(restoredCacheEntry);

      Assert.assertEquals(0, restoredPage.size());

      byteBufferPool.release(pointer);
      byteBufferPool.release(restoredPointer);
    } finally {
      byteBufferPool.clear();
    }
  }
}
