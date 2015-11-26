package com.orientechnologies.com.orientechnologies.common.directmemory;

import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(priority = Integer.MAX_VALUE)
public class DetectMemoryLeaksTest {
  @Test(priority = Integer.MAX_VALUE)
  public void testMemoryLeaks() throws Exception {
    System.gc();
    Thread.sleep(1000);
    Assert.assertEquals(ODirectMemoryPointerFactory.instance().getDetectedLeaks(), 0,
        "Memory leaks are detected. " + "For more details check console output.");
  }
}
