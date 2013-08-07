package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
@Test
public class LRUListTest {
  public void testSingleAdd() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointer = new OCachePointer(1);
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointer, false, new OLogSequenceNumber(0, 0)));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointer, false, new OLogSequenceNumber(0, 0)));
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1);
    OCachePointer cachePointerTwo = new OCachePointer(2);

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
  }

  public void testAddThree() {
    OCachePointer cachePointerOne = new OCachePointer(1);
    OCachePointer cachePointerTwo = new OCachePointer(2);
    OCachePointer cachePointerThree = new OCachePointer(3);

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
  }

  public void testAddThreePutMiddleToTop() {
    OCachePointer cachePointerOne = new OCachePointer(1);
    OCachePointer cachePointerTwo = new OCachePointer(2);
    OCachePointer cachePointerThree = new OCachePointer(3);

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));

    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
  }

  public void testAddThreePutMiddleToTopChangePointer() {

    OCachePointer cachePointerOne = new OCachePointer(1);
    OCachePointer cachePointerTwo = new OCachePointer(2);
    OCachePointer cachePointerThree = new OCachePointer(3);
    OCachePointer cachePointerFour = new OCachePointer(4);

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerTwo, false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));

    lruList.putToMRU(new OCacheEntry(1, 20, cachePointerFour, false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 20, cachePointerFour, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(3, 30, cachePointerThree, false, new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
  }

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    lruList.putToMRU(new OCacheEntry(1, 50, cachePointers[5], false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 50, cachePointers[5], false, new OLogSequenceNumber(0, 0)));

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAddOneRemoveLRU() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1);
    lruList.putToMRU(new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0)));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1);
    OCacheEntry cacheEntry = new OCacheEntry(1, 10, cachePointerOne, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(cacheEntry);
    cachePointerOne.incrementUsages();

    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    Assert.assertEquals(lruList.remove(1, 50), new OCacheEntry(1, 50, cachePointers[5], false, new OLogSequenceNumber(0, 0)));
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    Assert.assertEquals(lruList.get(1, 50), new OCacheEntry(1, 50, cachePointers[5], false, new OLogSequenceNumber(0, 0)));

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      Assert
          .assertEquals(lruList.get(1, i * 10), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    for (int i = 4564; i < 9128; i++)
      Assert.assertEquals(lruList.remove(1, i * 10), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0,
          0)));

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i);
      lruList.putToMRU(new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }

    lruList.putToMRU(new OCacheEntry(1, 0, cachePointers[0], false, new OLogSequenceNumber(0, 0)));
    lruList.putToMRU(new OCacheEntry(1, 4500 * 10, cachePointers[4500], false, new OLogSequenceNumber(0, 0)));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 4500 * 10, cachePointers[4500], false,
        new OLogSequenceNumber(0, 0)));
    Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, 0, cachePointers[0], false, new OLogSequenceNumber(0, 0)));

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntry(1, i * 10, cachePointers[i], false, new OLogSequenceNumber(0, 0)));
    }
  }
}
