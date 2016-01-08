package com.orientechnologies.orient.core.index.sbtree.local;

import java.nio.ByteBuffer;
import java.util.Random;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/1/13
 */
@Test
public class SBTreeValuePageTest {
  public void fillPageDataTest() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer bufferOne = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(bufferOne, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointerOne.incrementReferrer();

    OCacheEntry cacheEntryOne = new OCacheEntry(0, 0, cachePointerOne, false);
    OSBTreeValuePage valuePageOne = new OSBTreeValuePage(cacheEntryOne, null, true);

    byte[] data = new byte[ODurablePage.MAX_PAGE_SIZE_BYTES + 100];
    Random random = new Random();
    random.nextBytes(data);

    int offset = valuePageOne.fillBinaryContent(data, 0);
    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    ByteBuffer bufferTwo = bufferPool.acquireDirect(true);
    OCachePointer cachePointerTwo = new OCachePointer(bufferTwo, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointerTwo.incrementReferrer();

    OCacheEntry cacheEntryTwo = new OCacheEntry(0, 0, cachePointerTwo, false);
    OSBTreeValuePage valuePageTwo = new OSBTreeValuePage(cacheEntryTwo, null, true);
    offset = valuePageTwo.fillBinaryContent(data, offset);

    Assert.assertEquals(offset, data.length);

    valuePageOne.setNextPage(100);
    Assert.assertEquals(valuePageOne.getNextPage(), 100);

    byte[] readData = new byte[data.length];
    offset = valuePageOne.readBinaryContent(readData, 0);

    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    offset = valuePageTwo.readBinaryContent(readData, offset);
    Assert.assertEquals(offset, data.length);

    Assert.assertEquals(data, readData);

    cachePointerOne.decrementReferrer();
    cachePointerTwo.decrementReferrer();
  }

  public void testFreeListPointer() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);
    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, null, true);
    valuePage.setNextFreeListPage(124);
    Assert.assertEquals(valuePage.getNextFreeListPage(), 124);

    cachePointer.decrementReferrer();
  }
}
