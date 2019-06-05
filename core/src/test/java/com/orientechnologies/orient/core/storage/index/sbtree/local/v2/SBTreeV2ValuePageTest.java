package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/1/13
 */
public class SBTreeV2ValuePageTest {
  @Test
  public void fillPageDataTest() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointerOne = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointerOne, bufferPool, 0, 0);
    cachePointerOne.incrementReferrer();

    OCacheEntry cacheEntryOne = new OCacheEntryImpl(0, 0, cachePointerOne);
    cacheEntryOne.acquireExclusiveLock();
    OSBTreeValuePage valuePageOne = new OSBTreeValuePage(cacheEntryOne, true);

    byte[] data = new byte[ODurablePage.MAX_PAGE_SIZE_BYTES + 100];
    Random random = new Random();
    random.nextBytes(data);

    int offset = valuePageOne.fillBinaryContent(data, 0);
    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    OPointer pointerTwo = bufferPool.acquireDirect(true);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, bufferPool, 0, 0);
    cachePointerTwo.incrementReferrer();

    OCacheEntry cacheEntryTwo = new OCacheEntryImpl(0, 0, cachePointerTwo);
    cacheEntryTwo.acquireExclusiveLock();

    OSBTreeValuePage valuePageTwo = new OSBTreeValuePage(cacheEntryTwo, true);
    offset = valuePageTwo.fillBinaryContent(data, offset);

    Assert.assertEquals(offset, data.length);

    valuePageOne.setNextPage(100);
    Assert.assertEquals(valuePageOne.getNextPage(), 100);

    byte[] readData = new byte[data.length];
    offset = valuePageOne.readBinaryContent(readData, 0);

    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    offset = valuePageTwo.readBinaryContent(readData, offset);
    Assert.assertEquals(offset, data.length);

    Assertions.assertThat(data).isEqualTo(readData);
    cacheEntryOne.releaseExclusiveLock();
    cacheEntryTwo.releaseExclusiveLock();

    cachePointerOne.decrementReferrer();
    cachePointerTwo.decrementReferrer();
  }

  @Test
  public void testFreeListPointer() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer);
    cacheEntry.acquireExclusiveLock();

    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, true);
    valuePage.setNextFreeListPage(124);
    Assert.assertEquals(valuePage.getNextFreeListPage(), 124);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
