package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 20.03.13
 */
@Test
public class LocalPageTest {
  private ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  public void testAddOneRecord() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);
      int freeSpace = localPage.getFreeSpace();
      Assert.assertEquals(localPage.getRecordsCount(), 0);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

      int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      Assert.assertEquals(localPage.getRecordsCount(), 1);
      Assert.assertEquals(localPage.getRecordSize(0), 11);
      Assert.assertEquals(position, 0);
      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (23 + OVersionFactory.instance().getVersionSize()));
      Assert.assertFalse(localPage.isDeleted(0));
      Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddTreeRecords() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);
      int freeSpace = localPage.getFreeSpace();

      Assert.assertEquals(localPage.getRecordsCount(), 0);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

      int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

      Assert.assertEquals(localPage.getRecordsCount(), 3);
      Assert.assertEquals(positionOne, 0);
      Assert.assertEquals(positionTwo, 1);
      Assert.assertEquals(positionThree, 2);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (3 * (23 + OVersionFactory.instance().getVersionSize())));
      Assert.assertFalse(localPage.isDeleted(0));
      Assert.assertFalse(localPage.isDeleted(1));
      Assert.assertFalse(localPage.isDeleted(2));

      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      Assert.assertEquals(localPage.getRecordSize(0), 11);
      Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

      pointer = localPage.getRecordPointer(1);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      Assert.assertEquals(localPage.getRecordSize(0), 11);
      Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

      pointer = localPage.getRecordPointer(2);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
      Assert.assertEquals(localPage.getRecordSize(0), 11);
      Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddFullPage() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

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

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (15 + OVersionFactory.instance().getVersionSize()));
          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      Assert.assertEquals(localPage.getRecordsCount(), positions.size());

      counter = 0;
      for (int position : positions) {
        long pointer = localPage.getRecordPointer(position);

        Assert.assertEquals(directMemory.get(pointer, 3), new byte[] { counter, counter, counter });
        Assert.assertEquals(localPage.getRecordSize(position), 3);
        Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
        counter++;
      }

    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteAddLowerVersion() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();
      recordVersion.increment();

      int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

      Assert.assertTrue(localPage.deleteRecord(position));

      ORecordVersion newRecordVersion = OVersionFactory.instance().createVersion();

      Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

      long recordPointer = localPage.getRecordPointer(position);
      int recordSize = localPage.getRecordSize(position);
      Assert.assertEquals(recordSize, 11);

      recordVersion.increment();
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      Assert.assertEquals(directMemory.get(recordPointer, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteAddBiggerVersion() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();
      recordVersion.increment();

      int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

      Assert.assertTrue(localPage.deleteRecord(position));

      ORecordVersion newRecordVersion = OVersionFactory.instance().createVersion();
      newRecordVersion.increment();
      newRecordVersion.increment();
      newRecordVersion.increment();
      newRecordVersion.increment();

      Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

      long recordPointer = localPage.getRecordPointer(position);
      int recordSize = localPage.getRecordSize(position);
      Assert.assertEquals(recordSize, 11);

      Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
      Assert.assertEquals(directMemory.get(recordPointer, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteAddEqualVersion() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();
      recordVersion.increment();

      int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

      Assert.assertTrue(localPage.deleteRecord(position));

      Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

      long recordPointer = localPage.getRecordPointer(position);
      int recordSize = localPage.getRecordSize(position);
      Assert.assertEquals(recordSize, 11);

      recordVersion.increment();
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      Assert.assertEquals(directMemory.get(recordPointer, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteTwoOutOfFour() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

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

      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(pointer, ODirectMemory.NULL_POINTER);
      Assert.assertEquals(localPage.getRecordSize(0), -1);
      Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

      pointer = localPage.getRecordPointer(1);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      Assert.assertEquals(localPage.getRecordSize(1), 11);
      Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

      pointer = localPage.getRecordPointer(2);
      Assert.assertEquals(pointer, ODirectMemory.NULL_POINTER);
      Assert.assertEquals(localPage.getRecordSize(2), -1);
      Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

      pointer = localPage.getRecordPointer(3);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });
      Assert.assertEquals(localPage.getRecordSize(3), 11);
      Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

      Assert.assertEquals(localPage.getRecordsCount(), 2);
      Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 19 * 2);
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddFullPageDeleteAndAddAgain() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();
      Set<Integer> deletedPositions = new HashSet<Integer>();

      int lastPosition;
      byte counter = 0;
      int freeSpace = localPage.getFreeSpace();
      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

      do {
        lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          Assert.assertEquals(lastPosition, positionCounter.size());
          positionCounter.put(lastPosition, counter);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (15 + OVersionFactory.instance().getVersionSize()));
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

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 11);
          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      ORecordVersion deletedVersion = OVersionFactory.instance().createVersion();
      deletedVersion.copyFrom(recordVersion);

      deletedVersion.increment();

      Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
      for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
        final long pointer = localPage.getRecordPointer(entry.getKey());

        Assert.assertEquals(directMemory.get(pointer, 3), new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
        Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

        if (deletedPositions.contains(entry.getKey()))
          Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), deletedVersion);
        else
          Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);

      }
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddBigRecordDeleteAndAddSmallRecords() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();
      recordVersion.increment();

      final byte[] bigChunk = new byte[OLocalPage.MAX_ENTRY_SIZE / 2];
      final MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast();
      mersenneTwisterFast.nextBytes(bigChunk);

      int position = localPage.appendRecord(recordVersion, bigChunk);
      Assert.assertEquals(position, 0);
      Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

      Assert.assertTrue(localPage.deleteRecord(0));

      recordVersion.increment();
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
            Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 11);
          else
            Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (15 + OVersionFactory.instance().getVersionSize()));

          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
      for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
        final long pointer = localPage.getRecordPointer(entry.getKey());

        Assert.assertEquals(directMemory.get(pointer, 3), new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
        Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
      }
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testFindFirstRecord() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    final MersenneTwisterFast mersenneTwister = new MersenneTwisterFast();
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      Set<Integer> positions = new HashSet<Integer>();

      int lastPosition;
      byte counter = 0;
      int freeSpace = localPage.getFreeSpace();

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

      do {
        lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          Assert.assertEquals(lastPosition, positions.size());
          positions.add(lastPosition);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (15 + OVersionFactory.instance().getVersionSize()));
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
    } finally {
      directMemory.free(pagePointer);
    }

  }

  public void testFindLastRecord() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    final MersenneTwisterFast mersenneTwister = new MersenneTwisterFast();
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);

      Set<Integer> positions = new HashSet<Integer>();

      int lastPosition;
      byte counter = 0;
      int freeSpace = localPage.getFreeSpace();

      ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
      recordVersion.increment();

      do {
        lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          Assert.assertEquals(lastPosition, positions.size());
          positions.add(lastPosition);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (15 + OVersionFactory.instance().getVersionSize()));
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
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testSetGetNextPage() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);
      localPage.setNextPage(1034);
      Assert.assertEquals(localPage.getNextPage(), 1034);
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testSetGetPrevPage() throws Exception {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true, null, -1, null);
      localPage.setPrevPage(1034);
      Assert.assertEquals(localPage.getPrevPage(), 1034);
    } finally {
      directMemory.free(pagePointer);
    }
  }

}
