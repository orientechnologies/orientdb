/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.common.util;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Artem Loginov (logart2007-at-gmail.com)
 * @since 5/25/12 6:40 AM
 */
public class OByteBufferUtilsTest {

  private ByteBuffer buffer1;
  private ByteBuffer buffer2;

  @Before
  public void setUp() throws Exception {
    buffer1 = ByteBuffer.allocate(10);
    buffer2 = ByteBuffer.allocate(10);
  }

  @Test
  public void testSplitShort() throws Exception {
    short var = 42;
    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitShortToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    short storedVar = OByteBufferUtils.mergeShortFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    var = 251;
    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitShortToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeShortFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);
  }

  @Test
  public void testSplitLong() throws Exception {
    long var = 42;

    buffer1.position(3);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(3);
    buffer2.position(0);
    long storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(4);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(4);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(5);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(5);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(6);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(6);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(7);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(7);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(8);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(8);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);
    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    var = 2512513332512512344l;

    buffer1.position(3);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(3);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(4);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(4);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(5);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(5);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(6);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(6);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(7);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(7);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(8);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(8);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);
    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitLongToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeLongFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);
  }

  @Test
  public void testSplitInt() throws Exception {
    int var = 42;
    buffer1.position(7);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(7);
    buffer2.position(0);
    int storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(8);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(8);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    var = 251251333;
    buffer1.position(7);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(7);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(8);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(8);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);

    buffer1.position(9);
    buffer2.position(0);
    OByteBufferUtils.splitIntToBuffers(buffer1, buffer2, var);
    buffer1.position(9);
    buffer2.position(0);
    storedVar = OByteBufferUtils.mergeIntFromBuffers(buffer1, buffer2);

    assertEquals(storedVar, var);
  }

  @Test
  public void testSpecialSplitShort() throws Exception {
    byte[] array = new byte[10];
    ByteBuffer part1 = ByteBuffer.wrap(array, 0, 1);
    ByteBuffer part2 = ByteBuffer.wrap(array, 1, 1);
    ByteBuffer all = ByteBuffer.wrap(array, 0, 2);

    short value = Short.MAX_VALUE;
    OByteBufferUtils.splitShortToBuffers(part1, part2, value);

    all.position(0);

    short storedValue = all.getShort();

    assertEquals(value, storedValue);
  }

  @Test
  public void testSpecialSplitInteger() throws Exception {
    byte[] array = new byte[10];
    ByteBuffer part1 = ByteBuffer.wrap(array, 0, 2);
    ByteBuffer part2 = ByteBuffer.wrap(array, 2, 2);
    ByteBuffer all = ByteBuffer.wrap(array, 0, 4);

    int value = Integer.MAX_VALUE;
    OByteBufferUtils.splitIntToBuffers(part1, part2, value);

    all.position(0);

    int storedValue = all.getInt();

    assertEquals(value, storedValue);
  }

  @Test
  public void testSpecialSplitLong() throws Exception {
    byte[] array = new byte[10];
    ByteBuffer part1 = ByteBuffer.wrap(array, 0, 4);
    ByteBuffer part2 = ByteBuffer.wrap(array, 4, 4);
    ByteBuffer all = ByteBuffer.wrap(array, 0, 8);

    long value = Long.MAX_VALUE;
    OByteBufferUtils.splitLongToBuffers(part1, part2, value);

    all.position(0);

    long storedValue = all.getLong();

    assertEquals(value, storedValue);
  }
}
