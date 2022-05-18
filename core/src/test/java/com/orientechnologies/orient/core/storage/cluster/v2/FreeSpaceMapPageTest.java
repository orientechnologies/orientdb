package com.orientechnologies.orient.core.storage.cluster.v2;

import static org.junit.Assert.*;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import org.junit.Test;

public class FreeSpaceMapPageTest {

  @Test
  public void findSinglePageSameSpaceEvenIndex() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 3);
      assertEquals(42, page.findPage(3));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findSinglePageSameSpaceOddIndex() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(43, 3);
      assertEquals(43, page.findPage(3));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findSinglePageLessSpaceEvenIndex() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 3);
      assertEquals(42, page.findPage(2));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findSinglePageLessSpaceOddIndex() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(43, 3);
      assertEquals(43, page.findPage(2));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findCouplePagesSameSpaceOne() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 1);
      page.updatePageMaxFreeSpace(43, 3);
      assertEquals(43, page.findPage(3));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findCouplePagesSameSpaceTwo() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 1);
      page.updatePageMaxFreeSpace(43, 3);
      assertEquals(42, page.findPage(1));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findCouplePagesSmallerSpaceOne() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 1);
      page.updatePageMaxFreeSpace(43, 3);
      assertEquals(43, page.findPage(2));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void findCouplePagesSmallerSpaceTwo() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      page.updatePageMaxFreeSpace(42, 3);
      page.updatePageMaxFreeSpace(43, 5);
      assertEquals(42, page.findPage(2));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void bigSpaceOne() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);

      page.updatePageMaxFreeSpace(42, 128);
      page.updatePageMaxFreeSpace(43, 130);
      page.updatePageMaxFreeSpace(44, 132);

      assertEquals(43, page.findPage(129));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void bigSpaceTwo() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);

      page.updatePageMaxFreeSpace(42, 128);
      page.updatePageMaxFreeSpace(43, 130);
      page.updatePageMaxFreeSpace(44, 132);

      assertEquals(44, page.findPage(131));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void randomPages() {
    final int pages = 1_000;
    final int checks = 1_000;

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    final long seed = System.nanoTime();
    System.out.println("Random pages seed : " + seed);
    final Random random = new Random(seed);
    final HashMap<Integer, Integer> spacePageMap = new HashMap<>();
    int maxFreeSpace = -1;

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      for (int i = 0; i < pages; i++) {
        final int freeSpace = random.nextInt(256);
        spacePageMap.put(i, freeSpace);

        if (freeSpace > maxFreeSpace) {
          maxFreeSpace = freeSpace;
        }

        assertEquals(maxFreeSpace, page.updatePageMaxFreeSpace(i, freeSpace));
      }

      for (int i = 0; i < checks; i++) {
        final int freeSpace = random.nextInt(256);
        final int pageIndex = page.findPage(freeSpace);

        if (freeSpace <= maxFreeSpace) {
          assertTrue(spacePageMap.get(pageIndex) >= freeSpace);
        } else {
          assertEquals(-1, pageIndex);
        }
      }
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  @Test
  public void randomPagesUpdate() {
    final TreeMap<Integer, Integer> sizeCountMap = new TreeMap<>();
    final HashMap<Integer, Integer> spacePageMap = new HashMap<>();

    final int pages = 1_000;
    final int checks = 1_000;

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    final long seed = System.nanoTime();
    System.out.println("Random pages update seed : " + seed);

    final Random random = new Random(seed);

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
      for (int i = 0; i < pages; i++) {
        final int freeSpace = random.nextInt(256);
        spacePageMap.put(i, freeSpace);
        sizeCountMap.compute(
            freeSpace,
            (k, v) -> {
              if (v == null) {
                return 1;
              }

              return v + 1;
            });

        final int maxFreeSpace = sizeCountMap.lastKey();
        assertEquals(maxFreeSpace, page.updatePageMaxFreeSpace(i, freeSpace));
      }

      for (int i = 0; i < pages; i++) {
        final int freeSpace = random.nextInt(256);
        final int prevSpace = spacePageMap.get(i);
        spacePageMap.put(i, freeSpace);

        sizeCountMap.compute(
            prevSpace,
            (k, v) -> {
              //noinspection ConstantConditions
              if (v == 1) {
                return null;
              }

              return v - 1;
            });

        sizeCountMap.compute(
            freeSpace,
            (k, v) -> {
              if (v == null) {
                return 1;
              }

              return v + 1;
            });

        final int maxFreeSpace = sizeCountMap.lastKey();
        assertEquals(maxFreeSpace, page.updatePageMaxFreeSpace(i, freeSpace));
      }

      final int maxFreeSpace = sizeCountMap.lastKey();
      for (int i = 0; i < checks; i++) {
        final int freeSpace = random.nextInt(256);
        final int pageIndex = page.findPage(freeSpace);

        if (freeSpace <= maxFreeSpace) {
          assertTrue(spacePageMap.get(pageIndex) >= freeSpace);
        } else {
          assertEquals(-1, pageIndex);
        }
      }
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }
}
