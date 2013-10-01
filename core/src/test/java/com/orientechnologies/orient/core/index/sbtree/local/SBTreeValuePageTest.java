package com.orientechnologies.orient.core.index.sbtree.local;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/1/13
 */
@Test
public class SBTreeValuePageTest {
  private final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  public void fillPageDataTest() throws Exception {
    long pointerOne = directMemory.allocate(ODurablePage.MAX_PAGE_SIZE_BYTES);
    OSBTreeValuePage valuePageOne = new OSBTreeValuePage(pointerOne, ODurablePage.TrackMode.NONE, true);

    byte[] data = new byte[ODurablePage.MAX_PAGE_SIZE_BYTES + 100];
    Random random = new Random();
    random.nextBytes(data);

    int offset = valuePageOne.fillBinaryContent(data, 0);
    Assert.assertEquals(offset, OSBTreeValuePage.MAX_BINARY_VALUE_SIZE);

    long pointerTwo = directMemory.allocate(ODurablePage.MAX_PAGE_SIZE_BYTES);
    OSBTreeValuePage valuePageTwo = new OSBTreeValuePage(pointerTwo, ODurablePage.TrackMode.NONE, true);
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

    directMemory.free(pointerOne);
    directMemory.free(pointerTwo);
  }

  public void testFreeListPointer() throws Exception {
    long pointer = directMemory.allocate(ODurablePage.MAX_PAGE_SIZE_BYTES);
    OSBTreeValuePage valuePage = new OSBTreeValuePage(pointer, ODurablePage.TrackMode.NONE, true);
    valuePage.setNextFreeListPage(124);
    Assert.assertEquals(valuePage.getNextFreeListPage(), 124);

    directMemory.free(pointer);
  }
}
