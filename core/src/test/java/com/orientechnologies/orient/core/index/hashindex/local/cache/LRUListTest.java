package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

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

    lruList.putToMRU(1, 10, 100, false);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false);
    lruList.putToMRU(1, 20, 200, false);

    Assert.assertEquals(lruList.size(), 2);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), 1, 20, 200);
    assertLRUEntry(entryIterator.next(), 1, 10, 100);

  }

  public void testAddThree() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false);
    lruList.putToMRU(1, 20, 200, false);
    lruList.putToMRU(3, 30, 300, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), 3, 30, 300);
    assertLRUEntry(entryIterator.next(), 1, 20, 200);
    assertLRUEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddThreePutMiddleToTop() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false);
    lruList.putToMRU(1, 20, 200, false);
    lruList.putToMRU(3, 30, 300, false);

    lruList.putToMRU(1, 20, 200, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), 1, 20, 200);
    assertLRUEntry(entryIterator.next(), 3, 30, 300);
    assertLRUEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddThreePutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false);
    lruList.putToMRU(1, 20, 200, false);
    lruList.putToMRU(3, 30, 300, false);

    lruList.putToMRU(1, 20, 400, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), 1, 20, 400);
    assertLRUEntry(entryIterator.next(), 3, 30, 300);
    assertLRUEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    lruList.putToMRU(1, 50, 500, false);

    Assert.assertEquals(lruList.size(), 11);

    Iterator<LRUEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertLRUEntry(entryIterator.next(), 1, 50, 500);

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddOneRemoveLRU() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false);
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<LRUEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    assertLRUEntry(lruList.remove(1, 50), 1, 50, 500);
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    Assert.assertTrue(lruList.contains(1, 50));
    assertLRUEntry(lruList.get(1, 50), 1, 50, 500);

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      assertLRUEntry(lruList.get(1, i * 10), 1, i * 10, i * 100);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    for (int i = 4564; i < 9128; i++)
      assertLRUEntry(lruList.remove(1, i * 10), 1, i * 10, i * 100);

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false);
    }

    lruList.putToMRU(1, 0, 0, false);
    lruList.putToMRU(1, 4500 * 10, 4500 * 100, false);

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<LRUEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertLRUEntry(entryIterator.next(), 1, 4500 * 10, 4500 * 100);
    assertLRUEntry(entryIterator.next(), 1, 0, 0);

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  private void assertLRUEntry(LRUEntry lruEntry, long fileId, long filePosition, long dataPointer) {
    Assert.assertEquals(lruEntry.fileId, fileId);
    Assert.assertEquals(lruEntry.pageIndex, filePosition);
    Assert.assertEquals(lruEntry.dataPointer, dataPointer);
  }
}
