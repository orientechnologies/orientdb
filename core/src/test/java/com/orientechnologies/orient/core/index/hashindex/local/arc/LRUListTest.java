package com.orientechnologies.orient.core.index.hashindex.local.arc;

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

    lruList.putToMRU("f", 10, 100, false, false);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), "f", 10, 100);
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    lruList.putToMRU("f", 10, 100, false, false);
    lruList.putToMRU("f", 20, 200, false, false);

    Assert.assertEquals(lruList.size(), 2);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), "f", 20, 200);
    assertLRUEntry(entryIterator.next(), "f", 10, 100);

  }

  public void testAddThree() {
    LRUList lruList = new LRUList();

    lruList.putToMRU("f", 10, 100, false, false);
    lruList.putToMRU("f", 20, 200, false, false);
    lruList.putToMRU("f3", 30, 300, false, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), "f3", 30, 300);
    assertLRUEntry(entryIterator.next(), "f", 20, 200);
    assertLRUEntry(entryIterator.next(), "f", 10, 100);
  }

  public void testAddThreePutMiddleToTop() {
    LRUList lruList = new LRUList();

    lruList.putToMRU("f", 10, 100, false, false);
    lruList.putToMRU("f", 20, 200, false, false);
    lruList.putToMRU("f3", 30, 300, false, false);

    lruList.putToMRU("f", 20, 200, false, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), "f", 20, 200);
    assertLRUEntry(entryIterator.next(), "f3", 30, 300);
    assertLRUEntry(entryIterator.next(), "f", 10, 100);
  }

  public void testAddThreePutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    lruList.putToMRU("f", 10, 100, false, false);
    lruList.putToMRU("f", 20, 200, false, false);
    lruList.putToMRU("f3", 30, 300, false, false);

    lruList.putToMRU("f", 20, 400, false, false);

    Assert.assertEquals(lruList.size(), 3);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertLRUEntry(entryIterator.next(), "f", 20, 400);
    assertLRUEntry(entryIterator.next(), "f3", 30, 300);
    assertLRUEntry(entryIterator.next(), "f", 10, 100);
  }

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    lruList.putToMRU("f", 50, 500, false, false);

    Assert.assertEquals(lruList.size(), 11);

    Iterator<LRUEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertLRUEntry(entryIterator.next(), "f", 50, 500);

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAddOneRemoveLRU() {
    LRUList lruList = new LRUList();

    lruList.putToMRU("f", 10, 100, false, false);
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<LRUEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    assertLRUEntry(lruList.remove("f", 50), "f", 50, 500);
    Assert.assertNull(lruList.remove("f", 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    Assert.assertTrue(lruList.contains("f", 50));
    assertLRUEntry(lruList.get("f", 50), "f", 50, 500);

    Assert.assertFalse(lruList.contains("f2", 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      assertLRUEntry(lruList.get("f", i * 10), "f", i * 10, i * 100);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    for (int i = 4564; i < 9128; i++)
      assertLRUEntry(lruList.remove("f", i * 10), "f", i * 10, i * 100);

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<LRUEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU("f", i * 10, i * 100, false, false);
    }

    lruList.putToMRU("f", 0, 0, false, false);
    lruList.putToMRU("f", 4500 * 10, 4500 * 100, false, false);

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<LRUEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertLRUEntry(entryIterator.next(), "f", 4500 * 10, 4500 * 100);
    assertLRUEntry(entryIterator.next(), "f", 0, 0);

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertLRUEntry(entryIterator.next(), "f", i * 10, i * 100);
    }
  }

  private void assertLRUEntry(LRUEntry lruEntry, String fileName, long filePosition, long dataPointer) {
    Assert.assertEquals(lruEntry.fileName, fileName);
    Assert.assertEquals(lruEntry.pageIndex, filePosition);
    Assert.assertEquals(lruEntry.dataPointer, dataPointer);
  }
}
