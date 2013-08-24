package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;

@Test
public class PageChangesTest {
  public void testSingleValue() {
    OPageChanges pageChanges = new OPageChanges();
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(20);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 4), new byte[] { 0, 1, 2, 3 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 4), new byte[] { 3, 2, 1, 0 });

    directMemory.free(pointer);
  }

  public void testMultipleNotIntersectValues() {
    OPageChanges pageChanges = new OPageChanges();
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(20, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });
    pageChanges.addChanges(30, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 4), new byte[] { 0, 1, 2, 3 });
    Assert.assertEquals(directMemory.get(pointer + 20, 4), new byte[] { 4, 5, 6, 7 });
    Assert.assertEquals(directMemory.get(pointer + 30, 4), new byte[] { 8, 9, 10, 11 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 4), new byte[] { 3, 2, 1, 0 });
    Assert.assertEquals(directMemory.get(pointer + 20, 4), new byte[] { 7, 6, 5, 4 });
    Assert.assertEquals(directMemory.get(pointer + 30, 4), new byte[] { 11, 10, 9, 8 });

    directMemory.free(pointer);
  }

  public void testMultipleIntersectValues() {
    OPageChanges pageChanges = new OPageChanges();

    // 3, 2, 1, 0, 10
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });

    // 0, 1, 2, 3, 10
    pageChanges.addChanges(11, new byte[] { 4, 5, 6, 7 }, new byte[] { 1, 2, 3, 10 });
    // 0, 4, 5, 6, 7

    // 11, 10, 9, 8, 20
    pageChanges.addChanges(20, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });

    // 8, 9, 10, 11, 20
    pageChanges.addChanges(21, new byte[] { 12, 13, 14, 15 }, new byte[] { 9, 10, 11, 20 });
    // 8, 12, 13, 14, 15

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 5), new byte[] { 0, 4, 5, 6, 7 });
    Assert.assertEquals(directMemory.get(pointer + 20, 5), new byte[] { 8, 12, 13, 14, 15 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 5), new byte[] { 3, 2, 1, 0, 10 });
    Assert.assertEquals(directMemory.get(pointer + 20, 5), new byte[] { 11, 10, 9, 8, 20 });

    directMemory.free(pointer);
  }

  public void testMultipleAdjacentValues() {
    OPageChanges pageChanges = new OPageChanges();
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(14, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });
    pageChanges.addChanges(18, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 12), new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 12), new byte[] { 3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8 });

    directMemory.free(pointer);
  }

  public void testMultipleAdjacentValuesUpdateMixedOrder() {
    OPageChanges pageChanges = new OPageChanges();
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(22, new byte[] { 12, 13, 14, 15 }, new byte[] { 15, 14, 13, 12 });
    pageChanges.addChanges(18, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });
    pageChanges.addChanges(14, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 16), new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 16), new byte[] { 3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12 });

    directMemory.free(pointer);
  }

  public void testMultipleAdjacentValuesAndOneInTheMiddle() {
    OPageChanges pageChanges = new OPageChanges();

    // 3, 2, 1, 0, 7, 6, 5, 4,11, 10, 9, 8, 15, 14, 13, 12

    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(22, new byte[] { 12, 13, 14, 15 }, new byte[] { 15, 14, 13, 12 });
    pageChanges.addChanges(18, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });
    pageChanges.addChanges(14, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });

    // 0, 1, 2, 3, 4, 5, 6, 7,8, 9, 10, 11, 12, 13, 14, 15

    pageChanges.addChanges(13, new byte[] { 23, 24, 25 }, new byte[] { 3, 4, 5 });

    // 0, 1, 2, 23, 24, 25, 6, 7,8, 9, 10, 11, 12, 13, 14, 15

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 10, 16), new byte[] { 0, 1, 2, 23, 24, 25, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 10, 16), new byte[] { 3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12 });

    directMemory.free(pointer);
  }

  public void testMultipleAdjacentValuesAndOneBeforeThem() {
    OPageChanges pageChanges = new OPageChanges();

    // -2, -1, 3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(14, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });
    pageChanges.addChanges(18, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });
    // -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

    pageChanges.addChanges(8, new byte[] { 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111 }, new byte[] { -2,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 8, 14), new byte[] { 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
        110, 111 });

    pageChanges.revertChanges(pointer);
    Assert.assertEquals(directMemory.get(pointer + 8, 14), new byte[] { -2, -1, 3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8 });

    directMemory.free(pointer);
  }

  public void testMultipleNotIntersectValuesAndOneBeforeThem() {
    OPageChanges pageChanges = new OPageChanges();
    // -1, 3, 2, 1, 0,-2, 7, 6, 5, 4,-3, 11, 10, 9, 8
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });
    pageChanges.addChanges(15, new byte[] { 4, 5, 6, 7 }, new byte[] { 7, 6, 5, 4 });
    pageChanges.addChanges(20, new byte[] { 8, 9, 10, 11 }, new byte[] { 11, 10, 9, 8 });
    // -1, 0, 1, 2, 3,-2, 4, 5, 6, 7,-3, 8, 9, 10, 11

    for (byte i = 3; i < 17; i++) {
      pageChanges.addChanges(i * 10, new byte[] { i, (byte) (i + 1), (byte) (i + 2), (byte) (i + 3) }, new byte[] { (byte) (i + 3),
          (byte) (i + 2), (byte) (i + 1), i });
    }

    pageChanges.addChanges(9, new byte[] { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114 }, new byte[] {
        -1, 0, 1, 2, 3, -2, 4, 5, 6, 7, -3, 8, 9, 10, 11 });

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    long pointer = directMemory.allocate(1024);
    pageChanges.applyChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 9, 15), new byte[] { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
        112, 113, 114 });

    for (byte i = 3; i < 17; i++) {
      Assert.assertEquals(directMemory.get(pointer + i * 10, 4), new byte[] { i, (byte) (i + 1), (byte) (i + 2), (byte) (i + 3) });
    }

    pageChanges.revertChanges(pointer);

    Assert.assertEquals(directMemory.get(pointer + 9, 15), new byte[] { -1, 3, 2, 1, 0, -2, 7, 6, 5, 4, -3, 11, 10, 9, 8 });

    for (byte i = 3; i < 17; i++) {
      Assert.assertEquals(directMemory.get(pointer + i * 10, 4), new byte[] { (byte) (i + 3), (byte) (i + 2), (byte) (i + 1), i });
    }

    directMemory.free(pointer);
  }

  public void testAddOverlappedChangesInReverseOrder() {
    OPageChanges pageChanges = new OPageChanges();

    // 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
    pageChanges.addChanges(18, new byte[] { 28, 29, 30, 31 }, new byte[] { 18, 19, 20, 21 });

    // 10, 11, 12, 13, 14, 15, 16, 17, 28, 29, 30, 31
    pageChanges.addChanges(15, new byte[] { 35, 36, 37, 38 }, new byte[] { 15, 16, 17, 28 });

    // 10, 11, 12, 13, 14, 35, 36, 37, 38, 29, 30, 31
    pageChanges.addChanges(10, new byte[] { 0, 1, 2, 3 }, new byte[] { 3, 2, 1, 0 });

  }
}
