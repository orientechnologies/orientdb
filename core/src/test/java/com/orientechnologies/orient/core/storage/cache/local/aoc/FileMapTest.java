package com.orientechnologies.orient.core.storage.cache.local.aoc;

import org.junit.Assert;
import org.junit.Test;

public class FileMapTest {
  @Test
  public void testAllocate() {
    final FileMap fileMap = new FileMap();
    final int pageIndex = fileMap.allocateNewPage();

    Assert.assertEquals(0, pageIndex);
    Assert.assertEquals(-1, fileMap.mapIndex(0));
    Assert.assertArrayEquals(new int[] { -1, 0, 0 }, fileMap.fullMappingData(0));
  }

  @Test
  public void testMapping() {
    final FileMap fileMap = new FileMap();
    for (int i = 0; i < 64; i++) {
      final int pageIndex = fileMap.allocateNewPage();
      Assert.assertEquals(i, pageIndex);
    }

    for (int i = 0; i < 64; i++) {
      Assert.assertEquals(-1, fileMap.mapIndex(i));
      Assert.assertArrayEquals(new int[] { -1, 0, 0 }, fileMap.fullMappingData(i));
    }

    for (int i = 0; i < 64; i++) {
      fileMap.setMapping(i, 64 - i, i, i * 5);
    }

    for (int i = 0; i < 64; i++) {
      Assert.assertEquals(64 - i, fileMap.mapIndex(i));
      Assert.assertArrayEquals(new int[] { 64 - i, i, i * 5 }, fileMap.fullMappingData(i));
    }
  }
}
