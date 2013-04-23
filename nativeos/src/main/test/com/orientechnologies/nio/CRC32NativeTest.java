package com.orientechnologies.nio;

import java.util.Random;
import java.util.zip.CRC32;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin
 * @since 22.04.13
 */
@Test
public class CRC32NativeTest {
  @Test(enabled = false)
  public void testCRC32() {
    final Random random = new Random();
    CRC32 zipCRC32 = new CRC32();
    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    for (int i = 0; i < 16; i++) {
      int arraySize = 1 << i;
      byte[] testArray = new byte[arraySize];

      random.nextBytes(testArray);

      zipCRC32.reset();
      zipCRC32.update(testArray);

      long memoryPtr = directMemory.allocate(testArray);
      long nativeCRC32 = ONativeOS.crc32(new Pointer(memoryPtr), testArray.length);

      directMemory.free(memoryPtr);

      Assert.assertEquals(zipCRC32.getValue(), nativeCRC32);
    }
  }
}
