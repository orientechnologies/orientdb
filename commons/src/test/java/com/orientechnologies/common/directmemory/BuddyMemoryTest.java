/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.directmemory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Artem Orobets
 * @since 12.08.12
 */
public class BuddyMemoryTest {
  @Test
  public void testCapacityUpperBoundary() throws Exception {
    OBuddyMemory memory = new OBuddyMemory(191, 64);

    Assert.assertEquals(memory.capacity(), 128);
  }

  @Test
  public void testCapacityLowerBoundary() throws Exception {
    OBuddyMemory memory = new OBuddyMemory(129, 64);

    Assert.assertEquals(memory.capacity(), 128);
  }

  @Test
  public void testCapacity() throws Exception {
    OBuddyMemory memory = new OBuddyMemory(128, 64);

    Assert.assertEquals(memory.capacity(), 128);
  }

  @Test
  public void testAllocateInt() {
    OBuddyMemory memory = new OBuddyMemory(128, 64);

    int pointer1 = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
    int pointer2 = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
    int pointer3 = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);

    Assert.assertEquals(pointer1, 64);
    Assert.assertEquals(pointer2, 0);
    Assert.assertEquals(pointer3, ODirectMemory.NULL_POINTER);
  }

  @Test
  public void testAllocateTooMuch() {
    OBuddyMemory memory = new OBuddyMemory(128, 64);

    int pointer = memory.allocate(129);

    Assert.assertEquals(pointer, ODirectMemory.NULL_POINTER);
  }

  @Test
  public void testFreeSpace() throws Exception {
    OBuddyMemory memory = new OBuddyMemory(128, 64);
    int freeSpaceInEmptyMemmory = memory.freeSpace();
    Assert.assertEquals(freeSpaceInEmptyMemmory, 128);

    int pointer1 = memory.allocate(1);
    int freeSpaceAfterAllocation1 = memory.freeSpace();
    Assert.assertEquals(freeSpaceAfterAllocation1, 64);

    int pointer2 = memory.allocate(1);
    int freeSpaceAfterAllocation2 = memory.freeSpace();
    Assert.assertEquals(freeSpaceAfterAllocation2, 0);

    memory.free(pointer2);
    int freeSpaceAfterFree1 = memory.freeSpace();
    Assert.assertEquals(freeSpaceAfterFree1, 64);
    memory.free(pointer1);
    int freeSpaceAfterFree2 = memory.freeSpace();
    Assert.assertEquals(freeSpaceAfterFree2, 128);
  }

  @Test
  public void test960BytesMapping() throws Exception {
    final int expectedSize = 960;
    final OBuddyMemory memory = new OBuddyMemory(expectedSize, 64);
    Assert.assertEquals(memory.freeSpace(), expectedSize);

    final int chunkCount = expectedSize / 64;
    final List<Integer> pointers = new ArrayList<Integer>(chunkCount);

    for (int i = 0; i < chunkCount; i++) {
      int pointer = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
      Assert.assertTrue(pointer != ODirectMemory.NULL_POINTER);
      pointers.add(pointer);
    }

    int nullPointer = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
    Assert.assertTrue(nullPointer == ODirectMemory.NULL_POINTER);

    for (Integer pointer : pointers) {
      memory.free(pointer);
    }

    Assert.assertEquals(memory.freeSpace(), expectedSize);
  }

  @Test
  public void test704BytesMapping() throws Exception {
    final int expectedSize = 704;
    final OBuddyMemory memory = new OBuddyMemory(expectedSize, 64);
    Assert.assertEquals(memory.freeSpace(), expectedSize);

    final int chunkCount = expectedSize / 64;
    final List<Integer> pointers = new ArrayList<Integer>(chunkCount);

    for (int i = 0; i < chunkCount; i++) {
      int pointer = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
      Assert.assertTrue(pointer != ODirectMemory.NULL_POINTER);
      pointers.add(pointer);
    }

    int nullPointer = memory.allocate(64 - OBuddyMemory.SYSTEM_INFO_SIZE);
    Assert.assertTrue(nullPointer == ODirectMemory.NULL_POINTER);

    for (Integer pointer : pointers) {
      memory.free(pointer);
    }

    Assert.assertEquals(memory.freeSpace(), expectedSize);
  }

  @Test
  public void testWrites() throws Exception {
    final Random r = new Random();

    final int initialSize = 8192;
    final OBuddyMemory memory = new OBuddyMemory(initialSize, 64);

    int expectedSize = initialSize;
    Assert.assertEquals(memory.freeSpace(), expectedSize);

    final Map<Integer, byte[]> pointers = new HashMap<Integer, byte[]>();

    while (expectedSize > 256) {
      int arrayLen = 50 + r.nextInt(250);

      byte[] bytes = new byte[arrayLen];
      r.nextBytes(bytes);

      int pointer = memory.allocate(bytes);

      if (pointer == ODirectMemory.NULL_POINTER)
        continue;

      byte[] readBytes = memory.get(pointer, 0, arrayLen);
      Assert.assertEquals(readBytes, bytes);

      expectedSize -= memory.getActualSpace(pointer);
      Assert.assertEquals(memory.freeSpace(), expectedSize);

      if (r.nextDouble() < .8) {
        int actualSpace = memory.getActualSpace(pointer);
        memory.free(pointer);

        expectedSize += actualSpace;
        Assert.assertEquals(memory.freeSpace(), expectedSize);
      } else {
        pointers.put(pointer, bytes);
      }
    }

    for (Map.Entry<Integer, byte[]> entry : pointers.entrySet()) {
      final Integer pointer = entry.getKey();
      final byte[] expected = entry.getValue();

      final byte[] read = Arrays.copyOf(memory.get(pointer, 0, -1), expected.length);
      Assert.assertEquals(read, expected);

      final int actualSpace = memory.getActualSpace(pointer);
      memory.free(pointer);

      expectedSize += actualSpace;
      Assert.assertEquals(memory.freeSpace(), expectedSize);
    }

    Assert.assertEquals(memory.freeSpace(), initialSize);
  }
}
