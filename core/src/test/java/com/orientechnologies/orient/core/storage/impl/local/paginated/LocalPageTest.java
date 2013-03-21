package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 20.03.13
 */
@Test
public class LocalPageTest {
  private ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  public void testAddOneRecord() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);
      int freeSpace = localPage.getFreeSpace();
      Assert.assertEquals(freeSpace, OLocalPage.PAGE_SIZE - 5 * OIntegerSerializer.INT_SIZE);
      Assert.assertEquals(localPage.getRecordsCount(), 0);

      int position = localPage.appendRecord(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      Assert.assertEquals(localPage.getRecordsCount(), 1);
      Assert.assertEquals(position, 0);
      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 19);
      Assert.assertFalse(localPage.isDeleted(0));
      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddTreeRecords() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);
      int freeSpace = localPage.getFreeSpace();
      Assert.assertEquals(freeSpace, OLocalPage.PAGE_SIZE - 5 * OIntegerSerializer.INT_SIZE);
      Assert.assertEquals(localPage.getRecordsCount(), 0);

      int positionOne = localPage.appendRecord(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      int positionTwo = localPage.appendRecord(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      int positionThree = localPage.appendRecord(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

      Assert.assertEquals(localPage.getRecordsCount(), 3);
      Assert.assertEquals(positionOne, 0);
      Assert.assertEquals(positionTwo, 1);
      Assert.assertEquals(positionThree, 2);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 3 * 19);
      Assert.assertFalse(localPage.isDeleted(0));
      Assert.assertFalse(localPage.isDeleted(1));
      Assert.assertFalse(localPage.isDeleted(2));

      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

      pointer = localPage.getRecordPointer(1);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });

      pointer = localPage.getRecordPointer(2);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddFullPage() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);

      List<Integer> positions = new ArrayList<Integer>();
      int lastPosition;
      byte counter = 0;
      int freeSpace = localPage.getFreeSpace();
      Assert.assertEquals(freeSpace, OLocalPage.PAGE_SIZE - 5 * OIntegerSerializer.INT_SIZE);

      do {
        lastPosition = localPage.appendRecord(new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          Assert.assertEquals(lastPosition, positions.size());
          positions.add(lastPosition);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 11);
          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      Assert.assertEquals(localPage.getRecordsCount(), 5956);

      counter = 0;
      for (int position : positions) {
        long pointer = localPage.getRecordPointer(position);

        Assert.assertEquals(directMemory.get(pointer, 3), new byte[] { counter, counter, counter });
        counter++;
      }

    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteTwoOutOfFour() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);

      int positionOne = localPage.appendRecord(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      int positionTwo = localPage.appendRecord(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      int positionThree = localPage.appendRecord(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
      int positionFour = localPage.appendRecord(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

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

      Assert.assertTrue(localPage.markRecordAsDeleted(0));
      Assert.assertTrue(localPage.markRecordAsDeleted(2));

      Assert.assertFalse(localPage.markRecordAsDeleted(0));
      Assert.assertFalse(localPage.markRecordAsDeleted(7));

      Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
      Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
      Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

      long pointer = localPage.getRecordPointer(0);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

      pointer = localPage.getRecordPointer(1);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });

      pointer = localPage.getRecordPointer(2);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

      pointer = localPage.getRecordPointer(3);
      Assert.assertEquals(directMemory.get(pointer, 11), new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

      Assert.assertEquals(localPage.getRecordsCount(), 4);
      Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 15 * 2);
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testCleanOutOneOutOfFour() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);

      localPage.appendRecord(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      localPage.appendRecord(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      localPage.appendRecord(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
      localPage.appendRecord(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

      int freeSpace = localPage.getFreeSpace();

      localPage.cleanOutRecord(0);
      localPage.cleanOutRecord(2);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 15 * 2);
      freeSpace = localPage.getFreeSpace();

      Assert.assertFalse(localPage.markRecordAsDeleted(0));
      Assert.assertFalse(localPage.markRecordAsDeleted(2));

      Assert.assertTrue(localPage.isCleanedOut(0));
      Assert.assertTrue(localPage.isCleanedOut(2));

      Assert.assertEquals(localPage.getRecordsCount(), 2);

      int positionFive = localPage.appendRecord(new byte[] { 1, 2, 3 });
      Assert.assertEquals(positionFive, 2);

      int positionSix = localPage.appendRecord(new byte[] { 4, 5, 6 });
      Assert.assertEquals(positionSix, 0);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 2 * 7);
      Assert.assertEquals(localPage.getRecordsCount(), 4);
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testDeleteCleanOutOneOutOfFour() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);

      localPage.appendRecord(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
      localPage.appendRecord(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
      localPage.appendRecord(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
      localPage.appendRecord(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

      int freeSpace = localPage.getFreeSpace();

      Assert.assertTrue(localPage.markRecordAsDeleted(0));
      Assert.assertTrue(localPage.markRecordAsDeleted(2));

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 15 * 2);

      Assert.assertEquals(localPage.getRecordsCount(), 4);

      localPage.cleanOutRecord(0);
      localPage.cleanOutRecord(0);

      localPage.cleanOutRecord(2);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 15 * 2);
      freeSpace = localPage.getFreeSpace();

      Assert.assertTrue(localPage.isCleanedOut(0));
      Assert.assertTrue(localPage.isCleanedOut(2));

      Assert.assertEquals(localPage.getRecordPointer(0), ODirectMemory.NULL_POINTER);
      Assert.assertEquals(localPage.getRecordPointer(2), ODirectMemory.NULL_POINTER);

      Assert.assertEquals(localPage.getRecordsCount(), 2);

      int positionFive = localPage.appendRecord(new byte[] { 1, 2, 3 });
      Assert.assertEquals(positionFive, 2);

      int positionSix = localPage.appendRecord(new byte[] { 4, 5, 6 });
      Assert.assertEquals(positionSix, 0);

      Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 2 * 7);
      Assert.assertEquals(localPage.getRecordsCount(), 4);
    } finally {
      directMemory.free(pagePointer);
    }
  }

  public void testAddFullPageCleanOutAndAddAgain() {
    long pagePointer = directMemory.allocate(new byte[OLocalPage.PAGE_SIZE]);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, true);

      Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();

      int lastPosition;
      byte counter = 0;
      int freeSpace = localPage.getFreeSpace();
      Assert.assertEquals(freeSpace, OLocalPage.PAGE_SIZE - 5 * OIntegerSerializer.INT_SIZE);

      do {
        lastPosition = localPage.appendRecord(new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          Assert.assertEquals(lastPosition, positionCounter.size());
          positionCounter.put(lastPosition, counter);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 11);
          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      Assert.assertEquals(localPage.getRecordsCount(), 5956);

      for (int i = 0; i < 5956; i += 2) {
        localPage.cleanOutRecord(i);
        positionCounter.remove(i);
      }

      freeSpace = localPage.getFreeSpace();
      do {
        lastPosition = localPage.appendRecord(new byte[] { counter, counter, counter });
        if (lastPosition >= 0) {
          positionCounter.put(lastPosition, counter);
          counter++;

          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 7);
          freeSpace = localPage.getFreeSpace();
        }
      } while (lastPosition >= 0);

      Assert.assertEquals(localPage.getRecordsCount(), 5956);
      for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
        final long pointer = localPage.getRecordPointer(entry.getKey());

        Assert.assertEquals(directMemory.get(pointer, 3), new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      }
    } finally {
      directMemory.free(pagePointer);
    }
  }
}
