package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.orient.core.storage.cache.local.LRUList;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
public abstract class LRUListTest {

  protected LRUList lruList;

  @Test
  public void testSingleAdd() {
    ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);
    OCachePointer cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointer, false));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointer, false));
    directMemoryPointer.free();
  }

  @Test
  public void testAddTwo() {
    ODirectMemoryPointer directMemoryPointerOne = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0), 0, 0);

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false));

    directMemoryPointerOne.free();
    directMemoryPointerTwo.free();
  }

  @Test
  public void testAddThree() {
    ODirectMemoryPointer directMemoryPointerOne = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0), 0, 0);

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false));

    directMemoryPointerOne.free();
    directMemoryPointerTwo.free();
    directMemoryPointerThree.free();
  }

  @Test
  public void testAddThreePutMiddleToTop() {
    ODirectMemoryPointer directMemoryPointerOne = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0), 0, 0);

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false));

    directMemoryPointerOne.free();
    directMemoryPointerTwo.free();
    directMemoryPointerThree.free();
  }

  @Test
  public void testAddThreePutMiddleToTopChangePointer() {
    ODirectMemoryPointer directMemoryPointerOne = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = ODirectMemoryPointerFactory.instance().createPointer(1);
    ODirectMemoryPointer directMemoryPointerFour = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0), 0, 0);
    OCachePointer cachePointerFour = new OCachePointer(directMemoryPointerFour, new OLogSequenceNumber(0, 0), 0, 0);

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerFour, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerFour, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false));

    directMemoryPointerOne.free();
    directMemoryPointerTwo.free();
    directMemoryPointerThree.free();
    directMemoryPointerFour.free();
  }

  @Test
  public void testAddElevenPutMiddleToTopChangePointer() {
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OCacheEntry(1, 50, cachePointers[5], false));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 50, cachePointers[5], false));

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 0; i < 11; i++) {
      cachePointers[i].getDataPointer().free();
    }
  }

  @Test
  public void testAddOneRemoveLRU() {
    ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());

    directMemoryPointer.free();
  }

  @Test
  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
    OCacheEntry cacheEntry = new OCacheEntry(1, 10, cachePointerOne, false);
    lruList.putToMRU(cacheEntry);
    cacheEntry.incrementUsages();

    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);

    directMemoryPointer.free();
  }

  @Test
  public void testAddElevenRemoveLRU() {
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 0; i < 11; i++) {
      cachePointers[i].getDataPointer().free();
    }

  }

  @Test
  public void testAddElevenRemoveMiddle() {
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.remove(1, 50), new OCacheEntry(1, 50, cachePointers[5], false));
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 0; i < 11; i++) {
      cachePointers[i].getDataPointer().free();
    }
  }

  @Test
  public void testAddElevenGetMiddle() {
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    Assert.assertEquals(lruList.get(1, 50), new OCacheEntry(1, 50, cachePointers[5], false));

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 0; i < 11; i++) {
      cachePointers[i].getDataPointer().free();
    }
  }

  @Test
  public void testAdd9128() {
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (OCachePointer cachePointer : cachePointers)
      cachePointer.getDataPointer().free();
  }

  @Test
  public void testAdd9128Get() {
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      Assert.assertEquals(lruList.get(1, i * 10), new OCacheEntry(1, i * 10, cachePointers[i], false));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (OCachePointer cachePointer : cachePointers)
      cachePointer.getDataPointer().free();
  }

  @Test
  public void testAdd9128Remove4564() {
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 4564; i < 9128; i++)
      Assert.assertEquals(lruList.remove(1, i * 10), new OCacheEntry(1, i * 10, cachePointers[i], false));

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (OCachePointer cachePointer : cachePointers)
      cachePointer.getDataPointer().free();
  }

  @Test
  public void testAdd9128PutLastAndMiddleToTop() {
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0), 0, 0);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OCacheEntry(1, 0, cachePointers[0], false));
    lruList.putToMRU(new OCacheEntry(1, 4500 * 10, cachePointers[4500], false));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 4500 * 10, cachePointers[4500], false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 0, cachePointers[0], false));

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (OCachePointer cachePointer : cachePointers)
      cachePointer.getDataPointer().free();
  }
}
