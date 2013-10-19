package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
@Test
public class LRUListTest {
  public void testSingleAdd() {
    LRUList lruList = new LRUList();

    ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);
    OCachePointer cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointer, false));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointer, false));
    directMemoryPointer.free();
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    ODirectMemoryPointer directMemoryPointerOne = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = new ODirectMemoryPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0));

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

  public void testAddThree() {
    ODirectMemoryPointer directMemoryPointerOne = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = new ODirectMemoryPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

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

  public void testAddThreePutMiddleToTop() {
    ODirectMemoryPointer directMemoryPointerOne = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = new ODirectMemoryPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

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

  public void testAddThreePutMiddleToTopChangePointer() {
    ODirectMemoryPointer directMemoryPointerOne = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerTwo = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerThree = new ODirectMemoryPointer(1);
    ODirectMemoryPointer directMemoryPointerFour = new ODirectMemoryPointer(1);

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointerOne, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(directMemoryPointerTwo, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(directMemoryPointerThree, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerFour = new OCachePointer(directMemoryPointerFour, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

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

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAddOneRemoveLRU() {
    ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());

    directMemoryPointer.free();
  }

  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
    OCacheEntry cacheEntry = new OCacheEntry(1, 10, cachePointerOne, false);
    lruList.putToMRU(cacheEntry);
    cacheEntry.usagesCount++;

    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);

    directMemoryPointer.free();
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);

      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(1);
      cachePointers[i] = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
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
