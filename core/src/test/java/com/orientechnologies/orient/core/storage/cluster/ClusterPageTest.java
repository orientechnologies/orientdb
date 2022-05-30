package com.orientechnologies.orient.core.storage.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.03.13
 */
public class ClusterPageTest {
  private static final int SYSTEM_OFFSET = 24;

  @Test
  public void testAddOneRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();
      addOneRecord(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addOneRecord(OClusterPage localPage) {
    int freeSpace = localPage.getFreeSpace();
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 1;

    int position =
        localPage.appendRecord(
            recordVersion,
            new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1},
            -1,
            Collections.emptySet());
    Assert.assertEquals(localPage.getRecordsCount(), 1);
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(
        localPage.getFreeSpace(), freeSpace - (27 + ORecordVersionHelper.SERIALIZED_SIZE));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(0, 0, 11))
        .isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1});
  }

  @Test
  public void testAddThreeRecords() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addThreeRecords(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addThreeRecords(OClusterPage localPage) {
    int freeSpace = localPage.getFreeSpace();

    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int positionOne =
        localPage.appendRecord(
            recordVersion,
            new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1},
            -1,
            Collections.emptySet());
    int positionTwo =
        localPage.appendRecord(
            recordVersion,
            new byte[] {2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2},
            -1,
            Collections.emptySet());
    int positionThree =
        localPage.appendRecord(
            recordVersion,
            new byte[] {3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3},
            -1,
            Collections.emptySet());

    Assert.assertEquals(localPage.getRecordsCount(), 3);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);

    Assert.assertEquals(
        localPage.getFreeSpace(), freeSpace - (3 * (27 + ORecordVersionHelper.SERIALIZED_SIZE)));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));

    assertThat(localPage.getRecordBinaryValue(0, 0, 11))
        .isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1});
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(1, 0, 11))
        .isEqualTo(new byte[] {2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2});
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    assertThat(localPage.getRecordBinaryValue(2, 0, 11))
        .isEqualTo(new byte[] {3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3});
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);
  }

  @Test
  public void testAddFullPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addFullPage(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addFullPage(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;

    List<Integer> positions = new ArrayList<>();
    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positions.size());

    counter = 0;
    for (int position : positions) {
      assertThat(localPage.getRecordBinaryValue(position, 0, 3))
          .isEqualTo(new byte[] {counter, counter, counter});
      Assert.assertEquals(localPage.getRecordSize(position), 3);
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      counter++;
    }
  }

  @Test
  public void testAddDeleteAddBookedPositionsOne() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addDeleteAddBookedPositionsOne(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addDeleteAddBookedPositionsOne(final OClusterPage clusterPage) {
    final Set<Integer> bookedPositions = new HashSet<>();

    clusterPage.appendRecord(1, new byte[] {1}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {2}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {3}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {4}, -1, bookedPositions);

    clusterPage.deleteRecord(0, true);
    clusterPage.deleteRecord(1, true);
    clusterPage.deleteRecord(2, true);
    clusterPage.deleteRecord(3, true);

    bookedPositions.add(1);
    bookedPositions.add(2);

    int position = clusterPage.appendRecord(1, new byte[] {5}, -1, bookedPositions);
    Assert.assertEquals(3, position);

    position = clusterPage.appendRecord(1, new byte[] {6}, -1, bookedPositions);
    Assert.assertEquals(0, position);

    position = clusterPage.appendRecord(1, new byte[] {7}, -1, bookedPositions);
    Assert.assertEquals(4, position);

    position = clusterPage.appendRecord(1, new byte[] {8}, 1, bookedPositions);
    Assert.assertEquals(1, position);

    position = clusterPage.appendRecord(1, new byte[] {9}, 2, bookedPositions);
    Assert.assertEquals(2, position);

    position = clusterPage.appendRecord(1, new byte[] {10}, -1, bookedPositions);
    Assert.assertEquals(5, position);

    Assert.assertArrayEquals(new byte[] {6}, clusterPage.getRecordBinaryValue(0, 0, 1));
    Assert.assertArrayEquals(new byte[] {8}, clusterPage.getRecordBinaryValue(1, 0, 1));
    Assert.assertArrayEquals(new byte[] {9}, clusterPage.getRecordBinaryValue(2, 0, 1));
    Assert.assertArrayEquals(new byte[] {5}, clusterPage.getRecordBinaryValue(3, 0, 1));
    Assert.assertArrayEquals(new byte[] {7}, clusterPage.getRecordBinaryValue(4, 0, 1));
    Assert.assertArrayEquals(new byte[] {10}, clusterPage.getRecordBinaryValue(5, 0, 1));
  }

  @Test
  public void testAddDeleteAddBookedPositionsTwo() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addDeleteAddBookedPositionsTwo(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addDeleteAddBookedPositionsTwo(final OClusterPage clusterPage) {
    final Set<Integer> bookedPositions = new HashSet<>();

    clusterPage.appendRecord(1, new byte[] {1}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {2}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {3}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {4}, -1, bookedPositions);

    clusterPage.deleteRecord(0, true);
    clusterPage.deleteRecord(1, true);
    clusterPage.deleteRecord(2, true);
    clusterPage.deleteRecord(3, true);

    bookedPositions.add(1);
    bookedPositions.add(2);

    int position = clusterPage.appendRecord(1, new byte[] {5}, -1, bookedPositions);
    Assert.assertEquals(3, position);

    position = clusterPage.appendRecord(1, new byte[] {6}, -1, bookedPositions);
    Assert.assertEquals(0, position);

    position = clusterPage.appendRecord(1, new byte[] {9}, 2, bookedPositions);
    Assert.assertEquals(2, position);

    position = clusterPage.appendRecord(1, new byte[] {7}, -1, bookedPositions);
    Assert.assertEquals(4, position);

    position = clusterPage.appendRecord(1, new byte[] {8}, 1, bookedPositions);
    Assert.assertEquals(1, position);

    position = clusterPage.appendRecord(1, new byte[] {10}, -1, bookedPositions);
    Assert.assertEquals(5, position);

    Assert.assertArrayEquals(new byte[] {6}, clusterPage.getRecordBinaryValue(0, 0, 1));
    Assert.assertArrayEquals(new byte[] {8}, clusterPage.getRecordBinaryValue(1, 0, 1));
    Assert.assertArrayEquals(new byte[] {9}, clusterPage.getRecordBinaryValue(2, 0, 1));
    Assert.assertArrayEquals(new byte[] {5}, clusterPage.getRecordBinaryValue(3, 0, 1));
    Assert.assertArrayEquals(new byte[] {7}, clusterPage.getRecordBinaryValue(4, 0, 1));
    Assert.assertArrayEquals(new byte[] {10}, clusterPage.getRecordBinaryValue(5, 0, 1));
  }

  @Test
  public void testAddDeleteAddBookedPositionsThree() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addDeleteAddBookedPositionsThree(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addDeleteAddBookedPositionsThree(final OClusterPage clusterPage) {
    final Set<Integer> bookedPositions = new HashSet<>();

    clusterPage.appendRecord(1, new byte[] {1}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {2}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {3}, -1, bookedPositions);
    clusterPage.appendRecord(1, new byte[] {4}, -1, bookedPositions);

    clusterPage.deleteRecord(0, true);
    clusterPage.deleteRecord(1, true);
    clusterPage.deleteRecord(2, true);
    clusterPage.deleteRecord(3, true);

    bookedPositions.add(1);
    bookedPositions.add(2);

    int position = clusterPage.appendRecord(1, new byte[] {9}, 2, bookedPositions);
    Assert.assertEquals(2, position);

    position = clusterPage.appendRecord(1, new byte[] {8}, 1, bookedPositions);
    Assert.assertEquals(1, position);

    position = clusterPage.appendRecord(1, new byte[] {5}, -1, bookedPositions);
    Assert.assertEquals(3, position);

    position = clusterPage.appendRecord(1, new byte[] {6}, -1, bookedPositions);
    Assert.assertEquals(0, position);

    position = clusterPage.appendRecord(1, new byte[] {7}, -1, bookedPositions);
    Assert.assertEquals(4, position);

    position = clusterPage.appendRecord(1, new byte[] {10}, -1, bookedPositions);
    Assert.assertEquals(5, position);

    Assert.assertArrayEquals(new byte[] {6}, clusterPage.getRecordBinaryValue(0, 0, 1));
    Assert.assertArrayEquals(new byte[] {8}, clusterPage.getRecordBinaryValue(1, 0, 1));
    Assert.assertArrayEquals(new byte[] {9}, clusterPage.getRecordBinaryValue(2, 0, 1));
    Assert.assertArrayEquals(new byte[] {5}, clusterPage.getRecordBinaryValue(3, 0, 1));
    Assert.assertArrayEquals(new byte[] {7}, clusterPage.getRecordBinaryValue(4, 0, 1));
    Assert.assertArrayEquals(new byte[] {10}, clusterPage.getRecordBinaryValue(5, 0, 1));
  }

  @Test
  public void testDeleteAddLowerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddLowerVersion(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddLowerVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, true));

    int newRecordVersion = 0;

    Assert.assertEquals(
        localPage.appendRecord(
            newRecordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddLowerVersionNFL() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddLowerVersionNFL(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddLowerVersionNFL(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, false));

    int newRecordVersion = 0;

    Assert.assertEquals(
        localPage.appendRecord(
            newRecordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddBiggerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddBiggerVersion(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddBiggerVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, true));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(
        localPage.appendRecord(
            newRecordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddBiggerVersionNFL() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddBiggerVersionNFL(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddBiggerVersionNFL(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, false));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(
        localPage.appendRecord(
            newRecordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddEqualVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddEqualVersion(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, true));

    Assert.assertEquals(
        localPage.appendRecord(
            recordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddEqualVersionNFL() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddEqualVersionNFL(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersionNFL(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, false));

    Assert.assertEquals(
        localPage.appendRecord(
            recordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteAddEqualVersionKeepTombstoneVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteAddEqualVersionKeepTombstoneVersion(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersionKeepTombstoneVersion(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int position = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());

    Assert.assertArrayEquals(record, localPage.deleteRecord(position, true));

    Assert.assertEquals(
        localPage.appendRecord(
            recordVersion,
            new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2},
            -1,
            Collections.emptySet()),
        position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize))
        .isEqualTo(new byte[] {2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2});
  }

  @Test
  public void testDeleteTwoOutOfFour() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      deleteTwoOutOfFour(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void deleteTwoOutOfFour(OClusterPage localPage) {
    int recordVersion = 0;
    recordVersion++;

    final byte[] recordOne = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    final byte[] recordTwo = new byte[] {2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2};
    final byte[] recordThree = new byte[] {3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3};
    final byte[] recordFour = new byte[] {4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4};

    int positionOne = localPage.appendRecord(recordVersion, recordOne, -1, Collections.emptySet());
    int positionTwo = localPage.appendRecord(recordVersion, recordTwo, -1, Collections.emptySet());

    int positionThree =
        localPage.appendRecord(recordVersion, recordThree, -1, Collections.emptySet());
    int positionFour =
        localPage.appendRecord(recordVersion, recordFour, -1, Collections.emptySet());

    Assert.assertEquals(localPage.getRecordsCount(), 4);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);
    Assert.assertEquals(positionFour, 3);

    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));
    Assert.assertFalse(localPage.isDeleted(3));

    int freeSpace = localPage.getFreeSpace();

    Assert.assertArrayEquals(recordOne, localPage.deleteRecord(0, true));
    Assert.assertArrayEquals(recordThree, localPage.deleteRecord(2, true));

    Assert.assertNull(localPage.deleteRecord(0, true));
    Assert.assertNull(localPage.deleteRecord(7, true));

    Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
    Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
    Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

    Assert.assertTrue(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordSize(0), -1);
    Assert.assertEquals(localPage.getRecordVersion(0), -1);

    assertThat(localPage.getRecordBinaryValue(1, 0, 11))
        .isEqualTo(new byte[] {2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2});
    Assert.assertEquals(localPage.getRecordSize(1), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertTrue(localPage.isDeleted(2));
    Assert.assertEquals(localPage.getRecordSize(2), -1);
    Assert.assertEquals(localPage.getRecordVersion(2), -1);

    assertThat(localPage.getRecordBinaryValue(3, 0, 11))
        .isEqualTo(new byte[] {4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4});

    Assert.assertEquals(localPage.getRecordSize(3), 11);
    Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

    Assert.assertEquals(localPage.getRecordsCount(), 2);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 23 * 2);
  }

  @Test
  public void testAddFullPageDeleteAndAddAgain() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addFullPageDeleteAndAddAgain(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addFullPageDeleteAndAddAgain(OClusterPage localPage) {
    Map<Integer, Byte> positionCounter = new HashMap<>();
    Set<Integer> deletedPositions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i += 2) {
      localPage.deleteRecord(i, true);
      deletedPositions.add(i);
      positionCounter.remove(i);
    }

    freeSpace = localPage.getFreeSpace();
    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] {entry.getValue(), entry.getValue(), entry.getValue()});

      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

      if (deletedPositions.contains(entry.getKey()))
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  @Test
  public void testAddFullPageDeleteAndAddAgainNFL() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addFullPageDeleteAndAddAgainNFL(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addFullPageDeleteAndAddAgainNFL(OClusterPage localPage) {
    Map<Integer, Byte> positionCounter = new HashMap<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = filledRecordsCount; i >= 0; i--) {
      localPage.deleteRecord(i, false);
      positionCounter.remove(i);
    }

    freeSpace = localPage.getFreeSpace();
    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - 15 - OClusterPage.INDEX_ITEM_SIZE);
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] {entry.getValue(), entry.getValue(), entry.getValue()});

      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
    }
  }

  @Test
  public void testAddBigRecordDeleteAndAddSmallRecords() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    try {
      final long seed = System.currentTimeMillis();

      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      addBigRecordDeleteAndAddSmallRecords(seed, localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void addBigRecordDeleteAndAddSmallRecords(long seed, OClusterPage localPage) {
    final Random mersenneTwisterFast = new Random(seed);

    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] bigChunk = new byte[OClusterPage.MAX_ENTRY_SIZE / 2];

    mersenneTwisterFast.nextBytes(bigChunk);

    int position = localPage.appendRecord(recordVersion, bigChunk, -1, Collections.emptySet());
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertArrayEquals(bigChunk, localPage.deleteRecord(0, true));

    recordVersion++;
    int freeSpace = localPage.getFreeSpace();
    Map<Integer, Byte> positionCounter = new HashMap<>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        if (lastPosition == 0) Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        else
          Assert.assertEquals(
              localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));

        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] {entry.getValue(), entry.getValue(), entry.getValue()});
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
      Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  @Test
  public void testFindFirstRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      findFirstRecord(seed, localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void findFirstRecord(long seed, OClusterPage localPage) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i, true);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = 0;
    int lastRecordPosition = -1;

    do {
      recordPosition = localPage.findFirstRecord(recordPosition);
      if (recordPosition < 0) break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition > lastRecordPosition);

      lastRecordPosition = recordPosition;

      recordPosition++;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testFindLastRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      findLastRecord(seed, localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void findLastRecord(long seed, OClusterPage localPage) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition =
          localPage.appendRecord(
              recordVersion, new byte[] {counter, counter, counter}, -1, Collections.emptySet());
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(
            localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i, true);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = Integer.MAX_VALUE;
    int lastRecordPosition = Integer.MAX_VALUE;
    do {
      recordPosition = localPage.findLastRecord(recordPosition);
      if (recordPosition < 0) break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition < lastRecordPosition);

      recordPosition--;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testSetGetNextPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      setGetNextPage(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void setGetNextPage(OClusterPage localPage) {
    localPage.setNextPage(1034);
    Assert.assertEquals(localPage.getNextPage(), 1034);
  }

  @Test
  public void testSetGetPrevPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();
      setGetPrevPage(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void setGetPrevPage(OClusterPage localPage) {
    localPage.setPrevPage(1034);
    Assert.assertEquals(localPage.getPrevPage(), 1034);
  }

  @Test
  public void testReplaceOneRecordWithEqualSize() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      replaceOneRecordWithEqualSize(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithEqualSize(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int index = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    final byte[] oldRecord =
        localPage.replaceRecord(
            index, new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1}, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertArrayEquals(record, oldRecord);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 0, 11))
        .isEqualTo(new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1});
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordNoVersionUpdate() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      replaceOneRecordNoVersionUpdate(localPage);

    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordNoVersionUpdate(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int index = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());
    int freeSpace = localPage.getFreeSpace();

    byte[] oldRecord =
        localPage.replaceRecord(index, new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1}, -1);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertArrayEquals(record, oldRecord);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 0, 11))
        .isEqualTo(new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1});
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  @Test
  public void testReplaceOneRecordLowerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);

    OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false, null);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry);
      localPage.init();

      replaceOneRecordLowerVersion(localPage);
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordLowerVersion(OClusterPage localPage) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    final byte[] record = new byte[] {1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1};
    int index = localPage.appendRecord(recordVersion, record, -1, Collections.emptySet());
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion;
    newRecordVersion = recordVersion;

    byte[] oldRecord =
        localPage.replaceRecord(
            index, new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1}, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertArrayEquals(record, oldRecord);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 0, 11))
        .isEqualTo(new byte[] {5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1});
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  private byte[] getBytes(ByteBuffer buffer, int len) {
    byte[] result = new byte[len];
    buffer.position(ClusterPageTest.SYSTEM_OFFSET);
    buffer.get(result);

    return result;
  }
}
