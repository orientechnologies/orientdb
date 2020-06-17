package com.orientechnologies.orient.core.storage.cache.local.aoc;

import org.junit.Assert;
import org.junit.Test;

public class FileMapTest {
  @Test
  public void testAllocate() {
    final FileMap fileMap = new FileMap();
    final int pageIndex = fileMap.allocateNewPage();

    Assert.assertEquals(0, pageIndex);
    Assert.assertArrayEquals(new int[] {-1, 0, 0, 0}, fileMap.mappingData(0));
  }

  @Test
  public void testMapping() {
    final FileMap fileMap = new FileMap();
    for (int i = 0; i < 64; i++) {
      final int pageIndex = fileMap.allocateNewPage();
      Assert.assertEquals(i, pageIndex);
    }

    for (int i = 0; i < 64; i++) {
      Assert.assertArrayEquals(new int[] {-1, 0, 0, 0}, fileMap.mappingData(i));
    }

    for (int i = 0; i < 64; i++) {
      fileMap.setMapping(i, 64 - i, i + 1, FileMap.DATA_PAGE, i);
    }

    for (int i = 0; i < 64; i++) {
      Assert.assertArrayEquals(
          new int[] {64 - i, i + 1, FileMap.DATA_PAGE, i}, fileMap.mappingData(i));
    }
  }
}
