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
package com.orientechnologies.nio;

import java.util.Arrays;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OCharSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 5/6/13
 */

@Test
public class JNADirectMemoryTest {
  public void testLong() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    long value = rnd.nextLong();

    long pointer = directMemory.allocate(OLongSerializer.LONG_SIZE);
    directMemory.setLong(pointer, value);

    Assert.assertEquals(directMemory.getLong(pointer), value);

    directMemory.free(pointer);
  }

  public void testInt() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    int value = rnd.nextInt();
    long pointer = directMemory.allocate(OIntegerSerializer.INT_SIZE);
    directMemory.setInt(pointer, value);

    Assert.assertEquals(directMemory.getInt(pointer), value);

    directMemory.free(pointer);
  }

  public void testChar() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    char value = (char) rnd.nextInt();
    long pointer = directMemory.allocate(OCharSerializer.CHAR_SIZE);
    directMemory.setChar(pointer, value);

    Assert.assertEquals(directMemory.getChar(pointer), value);

    directMemory.free(pointer);
  }

  public void testByte() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[1];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(1);
    directMemory.setByte(pointer, value[0]);

    Assert.assertEquals(directMemory.getByte(pointer), value[0]);

    directMemory.free(pointer);
  }


  public void testBytesWithoutOffset() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[256];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(value.length);
    directMemory.set(pointer, value, 0, value.length);

    Assert.assertEquals(directMemory.get(pointer, value.length), value);
    Assert.assertEquals(directMemory.get(pointer, value.length / 2), Arrays.copyOf(value, value.length / 2));

    byte[] result = new byte[value.length];
    directMemory.get(pointer, result, value.length / 2, value.length / 2);

    byte[] expectedResult = new byte[value.length];
    System.arraycopy(value, 0, expectedResult, expectedResult.length / 2, expectedResult.length / 2);

    Assert.assertEquals(result, expectedResult);

    directMemory.free(pointer);
  }

  public void testBytesWithOffset() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[256];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(value.length);
    directMemory.set(pointer, value, value.length / 2, value.length / 2);

    Assert.assertEquals(directMemory.get(pointer, value.length / 2), Arrays.copyOfRange(value, value.length / 2, value.length));

    directMemory.free(pointer);
  }

  public void testCopyData() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[256];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(value.length);
    directMemory.set(pointer, value, 0, value.length);

    directMemory.moveData(pointer, pointer + value.length / 2, value.length / 2);

    System.arraycopy(value, 0, value, value.length / 2, value.length / 2);

    Assert.assertEquals(value, directMemory.get(pointer, value.length));

    directMemory.free(pointer);
  }

  public void testCopyDataOverlap() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[256];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(value.length);
    directMemory.set(pointer, value, 0, value.length);

    directMemory.moveData(pointer, pointer + 1, value.length / 3);

    System.arraycopy(value, 0, value, 1, value.length / 3);

    Assert.assertEquals(value, directMemory.get(pointer, value.length));

    directMemory.free(pointer);
  }

  public void testCopyDataOverlapInterval() {
    final Random rnd = new Random();
    ODirectMemory directMemory = new OJNADirectMemory();

    byte[] value = new byte[256];
    rnd.nextBytes(value);

    long pointer = directMemory.allocate(value.length);
    directMemory.set(pointer, value, 0, value.length);

    directMemory.moveData(pointer + 2, pointer + 5, value.length / 3);

    System.arraycopy(value, 2, value, 5, value.length / 3);

    Assert.assertEquals(value, directMemory.get(pointer, value.length));

    directMemory.free(pointer);
  }

}
