package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
@Test
public class LRUListTest {
  public void testSingleAdd() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointer = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointer, false));

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 10, cachePointer, false));
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(2, new OLogSequenceNumber(0, 0));

    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 10, cachePointerOne, false));
  }

  public void testAddThree() {
    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(2, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(3, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OReadCacheEntry(3, 30, cachePointerThree, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 10, cachePointerOne, false));
  }

  public void testAddThreePutMiddleToTop() {
    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(2, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(3, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OReadCacheEntry(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 10, cachePointerOne, false));
  }

  public void testAddThreePutMiddleToTopChangePointer() {

    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerTwo = new OCachePointer(2, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerThree = new OCachePointer(3, new OLogSequenceNumber(0, 0));
    OCachePointer cachePointerFour = new OCachePointer(4, new OLogSequenceNumber(0, 0));

    LRUList lruList = new LRUList();

    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OReadCacheEntry(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OReadCacheEntry(1, 20, cachePointerFour, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 20, cachePointerFour, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 10, cachePointerOne, false));
  }

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OReadCacheEntry(1, 50, cachePointers[5], false));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 50, cachePointers[5], false));

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAddOneRemoveLRU() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(new OReadCacheEntry(1, 10, cachePointerOne, false));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    LRUList lruList = new LRUList();

    OCachePointer cachePointerOne = new OCachePointer(1, new OLogSequenceNumber(0, 0));
    OReadCacheEntry cacheEntry = new OReadCacheEntry(1, 10, cachePointerOne, false);
    lruList.putToMRU(cacheEntry);
    cacheEntry.usagesCount++;

    OReadCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.remove(1, 50), new OReadCacheEntry(1, 50, cachePointers[5], false));
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    Assert.assertEquals(lruList.get(1, 50), new OReadCacheEntry(1, 50, cachePointers[5], false));

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      Assert.assertEquals(lruList.get(1, i * 10), new OReadCacheEntry(1, i * 10, cachePointers[i], false));

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    for (int i = 4564; i < 9128; i++)
      Assert.assertEquals(lruList.remove(1, i * 10), new OReadCacheEntry(1, i * 10, cachePointers[i], false));

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      cachePointers[i] = new OCachePointer(i, new OLogSequenceNumber(0, 0));
      lruList.putToMRU(new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OReadCacheEntry(1, 0, cachePointers[0], false));
    lruList.putToMRU(new OReadCacheEntry(1, 4500 * 10, cachePointers[4500], false));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OReadCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 4500 * 10, cachePointers[4500], false));
    Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, 0, cachePointers[0], false));

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OReadCacheEntry(1, i * 10, cachePointers[i], false));
    }
  }
}
