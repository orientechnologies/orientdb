package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.Random;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
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
    ODirectMemoryPointer pointerOne = ODirectMemoryPointerFactory.instance()
        .createPointer(ODurablePage.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointerOne = new OCachePointer(pointerOne, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointerOne.incrementReferrer();

    OCacheEntry cacheEntryOne = new OCacheEntry(0, 0, cachePointerOne, false);
    OSBTreeValuePage valuePageOne = new OSBTreeValuePage(cacheEntryOne, null, true);

    byte[] data = new byte[ODurablePage.MAX_PAGE_SIZE_BYTES + 100];
    Random random = new Random();
    random.nextBytes(data);

    int offset = valuePageOne.fillBinaryContent(data, 0);
    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    ODirectMemoryPointer pointerTwo = ODirectMemoryPointerFactory.instance().createPointer(ODurablePage.MAX_PAGE_SIZE_BYTES);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, new OLogSequenceNumber(0, 0), 0, 0);
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
    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance()
        .createPointer(ODurablePage.MAX_PAGE_SIZE_BYTES + ODurablePage.PAGE_PADDING);
    OCachePointer cachePointer = new OCachePointer(pointer, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);
    OSBTreeValuePage valuePage = new OSBTreeValuePage(cacheEntry, null, true);
    valuePage.setNextFreeListPage(124);
    Assert.assertEquals(valuePage.getNextFreeListPage(), 124);

    cachePointer.decrementReferrer();
  }
}
