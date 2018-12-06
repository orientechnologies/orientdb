package com.orientechnologies.orient.core.storage.index.hashindex.local.cache;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.twoq.LRUList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.02.13
 */
public abstract class AbstractLRUListTestTemplate {

  protected LRUList lruList;

  @Test
  public void testSingleAdd() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    final OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointer));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointer));
    bufferPool.release(pointer);
    bufferPool.clear();
  }

  @Test
  public void testAddTwo() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OPointer pointerOne = bufferPool.acquireDirect(true);
    OPointer pointerTwo = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne));

    bufferPool.release(pointerOne);
    bufferPool.release(pointerTwo);
    bufferPool.clear();
  }

  @Test
  public void testAddThree() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OPointer pointerOne = bufferPool.acquireDirect(true);
    OPointer pointerTwo = bufferPool.acquireDirect(true);
    OPointer pointerThree = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(pointerThree, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne));

    bufferPool.release(pointerOne);
    bufferPool.release(pointerTwo);
    bufferPool.release(pointerThree);

    bufferPool.clear();
  }

  @Test
  public void testAddThreePutMiddleToTop() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OPointer pointerOne = bufferPool.acquireDirect(true);
    OPointer pointerTwo = bufferPool.acquireDirect(true);
    OPointer pointerThree = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(pointerThree, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree));

    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne));

    bufferPool.release(pointerOne);
    bufferPool.release(pointerTwo);
    bufferPool.release(pointerThree);

    bufferPool.clear();
  }

  @Test
  public void testAddThreePutMiddleToTopChangePointer() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OPointer pointerOne = bufferPool.acquireDirect(true);
    OPointer pointerTwo = bufferPool.acquireDirect(true);
    OPointer pointerThree = bufferPool.acquireDirect(true);
    OPointer pointerFour = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(pointerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(pointerThree, bufferPool, 0, 0);
    OCachePointer cachePointerFour = new OCachePointer(pointerFour, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree));

    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerFour));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerFour));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne));

    bufferPool.release(pointerOne);
    bufferPool.release(pointerTwo);
    bufferPool.release(pointerThree);
    bufferPool.release(pointerFour);

    bufferPool.clear();
  }

  @Test
  public void testAddElevenPutMiddleToTopChangePointer() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);

      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    lruList.putToMRU(new OCacheEntryImpl(1, 50, cachePointers[5]));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 50, cachePointers[5]));

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer pointer : cachePointers) {
      bufferPool.release(pointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testAddOneRemoveLRU() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointer, bufferPool, 0, 0);
    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());

    bufferPool.release(pointer);
    bufferPool.clear();
  }

  @Test
  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OPointer pointer = bufferPool.acquireDirect(true);

    OCachePointer cachePointerOne = new OCachePointer(pointer, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(1, 10, cachePointerOne);
    lruList.putToMRU(cacheEntry);
    cacheEntry.incrementUsages();

    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);

    bufferPool.release(pointer);
    bufferPool.clear();
  }

  @Test
  public void testAddElevenRemoveLRU() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);

      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testAddElevenRemoveMiddle() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);

      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    Assert.assertEquals(lruList.remove(1, 50), new OCacheEntryImpl(1, 50, cachePointers[5]));
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }
    bufferPool.clear();
  }

  @Test
  public void testAddElevenGetMiddle() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OPointer buffer = bufferPool.acquireDirect(true);

      cachePointers[i] = new OCachePointer(buffer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    Assert.assertEquals(lruList.get(1, 50), new OCacheEntryImpl(1, 50, cachePointers[5]));

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }
    bufferPool.clear();
  }

  @Test
  public void testAdd9128() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);
      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testAdd9128Get() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);
      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      Assert.assertEquals(lruList.get(1, i * 10), new OCacheEntryImpl(1, i * 10, cachePointers[i]));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer pointer : cachePointers) {
      bufferPool.release(pointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testAdd9128Remove4564() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);
      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (int i = 4564; i < 9128; i++)
      Assert.assertEquals(lruList.remove(1, i * 10), new OCacheEntryImpl(1, i * 10, cachePointers[i]));

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testAdd9128PutLastAndMiddleToTop() {
    final OByteBufferPool bufferPool = new OByteBufferPool(1);

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OPointer pointer = bufferPool.acquireDirect(true);
      cachePointers[i] = new OCachePointer(pointer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    lruList.putToMRU(new OCacheEntryImpl(1, 0, cachePointers[0]));
    lruList.putToMRU(new OCacheEntryImpl(1, 4500 * 10, cachePointers[4500]));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 4500 * 10, cachePointers[4500]));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 0, cachePointers[0]));

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i]));
    }

    for (OCachePointer cachePointer : cachePointers) {
      bufferPool.release(cachePointer.getPointer());
    }

    bufferPool.clear();
  }

  @Test
  public void testInverseIterator() {
    final ArrayList<OCacheEntry> entries = new ArrayList<OCacheEntry>();

    for (int i = 0; i < 10; i++) {
      final OCacheEntry cacheEntry = new OCacheEntryImpl(1, i, null);

      entries.add(cacheEntry);
      lruList.putToMRU(cacheEntry);
    }

    final Iterator<OCacheEntry> reverseIterator = lruList.reverseIterator();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(reverseIterator.hasNext());
      final OCacheEntry cacheEntry = reverseIterator.next();
      Assert.assertEquals(entries.get(i), cacheEntry);
      Assert.assertTrue(i < 9 == reverseIterator.hasNext());
    }
  }
}
