package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OCacheEntryChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin
 * @since 20.03.13
 */
public class ClusterPageTest {
  private static final int SYSTEM_OFFSET = 24;

  @Test
  public void testAddOneRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addOneRecord(localPage);
      addOneRecord(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addOneRecord(OClusterPage localPage) throws IOException {
    int freeSpace = localPage.getFreeSpace();
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 1;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordsCount(), 1);
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (27 + ORecordVersionHelper.SERIALIZED_SIZE));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(0, 0, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    assertThat(localPage.getRecordBinaryValue(0, 0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
  }

  @Test
  public void testAddThreeRecords() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addThreeRecords(localPage);
      addThreeRecords(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addThreeRecords(OClusterPage localPage) throws IOException {
    int freeSpace = localPage.getFreeSpace();

    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

    Assert.assertEquals(localPage.getRecordsCount(), 3);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);

    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (3 * (27 + ORecordVersionHelper.SERIALIZED_SIZE)));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));

    //    Assert.assertEquals(localPage.getRecordBinaryValue(0, 0, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    assertThat(localPage.getRecordBinaryValue(0, 0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(1, 0, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    assertThat(localPage.getRecordBinaryValue(1, 0, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(2, 0, 11), new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    assertThat(localPage.getRecordBinaryValue(2, 0, 11)).isEqualTo(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);
  }

  @Test
  public void testAddFullPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addFullPage(localPage);
      addFullPage(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPage(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;

    List<Integer> positions = new ArrayList<Integer>();
    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positions.size());

    counter = 0;
    for (int position : positions) {
      //      Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, 3), new byte[] { counter, counter, counter });
      assertThat(localPage.getRecordBinaryValue(position, 0, 3)).isEqualTo(new byte[] { counter, counter, counter });
      Assert.assertEquals(localPage.getRecordSize(position), 3);
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      counter++;
    }
  }

  @Test
  public void testDeleteAddLowerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      deleteAddLowerVersion(localPage);
      deleteAddLowerVersion(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddLowerVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    //    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddBiggerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      deleteAddBiggerVersion(localPage);
      deleteAddBiggerVersion(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddBiggerVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    //    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });

    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      deleteAddEqualVersion(localPage);
      deleteAddEqualVersion(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    //    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersionKeepTombstoneVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      deleteAddEqualVersionKeepTombstoneVersion(localPage);
      deleteAddEqualVersionKeepTombstoneVersion(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersionKeepTombstoneVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    //    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
    assertThat(localPage.getRecordBinaryValue(position, 0, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteTwoOutOfFour() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      deleteTwoOutOfFour(localPage);
      deleteTwoOutOfFour(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteTwoOutOfFour(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    int positionFour = localPage.appendRecord(recordVersion, new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

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

    Assert.assertTrue(localPage.deleteRecord(0));
    Assert.assertTrue(localPage.deleteRecord(2));

    Assert.assertFalse(localPage.deleteRecord(0));
    Assert.assertFalse(localPage.deleteRecord(7));

    Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
    Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
    Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

    Assert.assertTrue(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordSize(0), -1);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(1, 0, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });

    assertThat(localPage.getRecordBinaryValue(1, 0, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(1), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertTrue(localPage.isDeleted(2));
    Assert.assertEquals(localPage.getRecordSize(2), -1);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(3, 0, 11), new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });
    assertThat(localPage.getRecordBinaryValue(3, 0, 11)).isEqualTo(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordSize(3), 11);
    Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

    Assert.assertEquals(localPage.getRecordsCount(), 2);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 23 * 2);
  }

  @Test
  public void testAddFullPageDeleteAndAddAgain() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addFullPageDeleteAndAddAgain(localPage);
      addFullPageDeleteAndAddAgain(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPageDeleteAndAddAgain(OClusterPage localPage) throws IOException {
    Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();
    Set<Integer> deletedPositions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i += 2) {
      localPage.deleteRecord(i);
      deletedPositions.add(i);
      positionCounter.remove(i);
    }

    freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      //      Assert.assertEquals(localPage.getRecordBinaryValue(entry.getKey(), 0, 3),
      //          new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });

      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });

      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

      if (deletedPositions.contains(entry.getKey()))
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);

    }
  }

  @Test
  public void testAddBigRecordDeleteAndAddSmallRecords() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      final long seed = System.currentTimeMillis();

      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addBigRecordDeleteAndAddSmallRecords(seed, localPage);
      addBigRecordDeleteAndAddSmallRecords(seed, directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addBigRecordDeleteAndAddSmallRecords(long seed, OClusterPage localPage) throws IOException {
    final Random mersenneTwisterFast = new Random(seed);

    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] bigChunk = new byte[OClusterPage.MAX_ENTRY_SIZE / 2];

    mersenneTwisterFast.nextBytes(bigChunk);

    int position = localPage.appendRecord(recordVersion, bigChunk);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertTrue(localPage.deleteRecord(0));

    recordVersion++;
    int freeSpace = localPage.getFreeSpace();
    Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        if (lastPosition == 0)
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        else
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));

        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      //      Assert.assertEquals(localPage.getRecordBinaryValue(entry.getKey(), 0, 3),
      //          new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });

      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 0, 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
      Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  @Test
  public void testFindFirstRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      findFirstRecord(seed, localPage);
      findFirstRecord(seed, directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void findFirstRecord(long seed, OClusterPage localPage) throws IOException {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = 0;
    int lastRecordPosition = -1;

    do {
      recordPosition = localPage.findFirstRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition > lastRecordPosition);

      lastRecordPosition = recordPosition;

      recordPosition++;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testFindLastRecord() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      findLastRecord(seed, localPage);
      findLastRecord(seed, directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void findLastRecord(long seed, OClusterPage localPage) throws IOException {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = Integer.MAX_VALUE;
    int lastRecordPosition = Integer.MAX_VALUE;
    do {
      recordPosition = localPage.findLastRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition < lastRecordPosition);

      recordPosition--;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testSetGetNextPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      setGetNextPage(localPage);
      setGetNextPage(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetNextPage(OClusterPage localPage) throws IOException {
    localPage.setNextPage(1034);
    Assert.assertEquals(localPage.getNextPage(), 1034);
  }

  @Test
  public void testSetGetPrevPage() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      setGetPrevPage(localPage);
      setGetPrevPage(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetPrevPage(OClusterPage localPage) throws IOException {
    localPage.setPrevPage(1034);
    Assert.assertEquals(localPage.getPrevPage(), 1034);
  }

  @Test
  public void testReplaceOneRecordWithBiggerSize() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordWithBiggerSize(localPage);
      replaceOneRecordWithBiggerSize(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithBiggerSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordWithEqualSize() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordWithEqualSize(localPage);
      replaceOneRecordWithEqualSize(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithEqualSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordWithSmallerSize() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordWithSmallerSize(localPage);
      replaceOneRecordWithSmallerSize(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithSmallerSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 6);

    Assert.assertEquals(localPage.getRecordSize(index), 6);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 6), new byte[] { 5, 2, 3, 4, 5, 11 });

    assertThat(localPage.getRecordBinaryValue(index, 0, 6)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordNoVersionUpdate() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordNoVersionUpdate(localPage);
      replaceOneRecordNoVersionUpdate(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordNoVersionUpdate(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, -1);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    //    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  @Test
  public void testReplaceOneRecordLowerVersion() throws Exception {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer buffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntryImpl(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage localPage = new OClusterPage(new OCacheEntryChanges(cacheEntry), true);
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordLowerVersion(localPage);
      replaceOneRecordLowerVersion(directLocalPage);

      assertChangesTracking(localPage, directBuffer, bufferPool);
    } finally {
      cacheEntry.releaseExclusiveLock();
      directCacheEntry.releaseExclusiveLock();

      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordLowerVersion(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);
    //    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    assertThat(localPage.getRecordBinaryValue(index, 0, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  private void assertChangesTracking(OClusterPage localPage, ByteBuffer buffer, OByteBufferPool bufferPool) throws IOException {
    ByteBuffer restoredBuffer = bufferPool.acquireDirect(true);

    OCachePointer cachePointer = new OCachePointer(restoredBuffer, bufferPool, new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(cacheEntry, false);

      OWALChanges changes = localPage.getChanges();
      restoredPage.restoreChanges(changes);

      //      Assert.assertEquals(getBytes(restoredBuffer, SYSTEM_OFFSET, OClusterPage.PAGE_SIZE - SYSTEM_OFFSET),
      //          getBytes(buffer, SYSTEM_OFFSET, OClusterPage.PAGE_SIZE - SYSTEM_OFFSET));

      assertThat(getBytes(restoredBuffer, SYSTEM_OFFSET, OClusterPage.PAGE_SIZE - SYSTEM_OFFSET))
          .isEqualTo(getBytes(buffer, SYSTEM_OFFSET, OClusterPage.PAGE_SIZE - SYSTEM_OFFSET));
    } finally {
      cacheEntry.releaseExclusiveLock();
      cachePointer.decrementReferrer();
    }
  }

  private byte[] getBytes(ByteBuffer buffer, int position, int len) {
    byte[] result = new byte[len];
    buffer.position(position);
    buffer.get(result);

    return result;
  }
}
