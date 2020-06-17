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

package com.orientechnologies.orient.core.record.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** @author bogdan */
public class ORecordBytesTest {
  private static final int SMALL_ARRAY = 3;
  private static final int BIG_ARRAY = 7;
  private static final int FULL_ARRAY = 5;
  private InputStream inputStream;
  private InputStream emptyStream;
  private OBlob testedInstance;

  private static void assertArrayEquals(byte[] actual, byte[] expected) {
    assert actual.length == expected.length;
    for (int i = 0; i < expected.length; i++) {
      assert actual[i] == expected[i];
    }
  }

  private static Object getFieldValue(Object source, String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    final Class<?> clazz = source.getClass();
    final Field field = getField(clazz, fieldName);
    field.setAccessible(true);
    return field.get(source);
  }

  private static Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    if (clazz == null) {
      throw new NoSuchFieldException(fieldName);
    }
    for (Field item : clazz.getDeclaredFields()) {
      if (item.getName().equals(fieldName)) {
        return item;
      }
    }
    return getField(clazz.getSuperclass(), fieldName);
  }

  @Before
  public void setUp() throws Exception {
    inputStream = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5});
    emptyStream = new ByteArrayInputStream(new byte[] {});
    testedInstance = new ORecordBytes();
  }

  @Test
  public void testFromInputStream_ReadEmpty() throws Exception {
    final int result = testedInstance.fromInputStream(emptyStream, SMALL_ARRAY);
    Assert.assertEquals(result, 0);
    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    Assert.assertEquals(source.length, 0);
  }

  @Test
  public void testFromInputStream_ReadSmall() throws Exception {
    final int result = testedInstance.fromInputStream(inputStream, SMALL_ARRAY);
    Assert.assertEquals(result, SMALL_ARRAY);
    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    Assert.assertEquals(source.length, SMALL_ARRAY);
    for (int i = 1; i < SMALL_ARRAY + 1; i++) {
      Assert.assertEquals(source[i - 1], i);
    }
  }

  @Test
  public void testFromInputStream_ReadBig() throws Exception {
    final int result = testedInstance.fromInputStream(inputStream, BIG_ARRAY);
    Assert.assertEquals(result, FULL_ARRAY);
    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    Assert.assertEquals(source.length, FULL_ARRAY);
    for (int i = 1; i < FULL_ARRAY + 1; i++) {
      Assert.assertEquals(source[i - 1], i);
    }
  }

  @Test
  public void testFromInputStream_ReadFull() throws Exception {
    final int result = testedInstance.fromInputStream(inputStream, FULL_ARRAY);
    Assert.assertEquals(result, FULL_ARRAY);
    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    Assert.assertEquals(source.length, FULL_ARRAY);
    for (int i = 1; i < FULL_ARRAY + 1; i++) {
      Assert.assertEquals(source[i - 1], i);
    }
  }

  @Test
  public void testReadFromInputStreamWithWait() throws Exception {
    final byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    final InputStream is = new NotFullyAvailableAtTheTimeInputStream(data, 5);

    final int result = testedInstance.fromInputStream(is);
    Assert.assertEquals(result, data.length);
    Assert.assertEquals(
        (Integer) getFieldValue(testedInstance, "size"), Integer.valueOf(data.length));

    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    assertArrayEquals(source, data);
  }

  @Test
  public void testReadFromInputStreamWithWaitSizeLimit() throws Exception {
    final byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    final InputStream is = new NotFullyAvailableAtTheTimeInputStream(data, 5);

    final int result = testedInstance.fromInputStream(is, 10);
    Assert.assertEquals(result, data.length);
    Assert.assertEquals(
        (Integer) getFieldValue(testedInstance, "size"), Integer.valueOf(data.length));

    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    assertArrayEquals(source, data);
  }

  @Test
  public void testReadFromInputStreamWithWaitSizeTooBigLimit() throws Exception {
    final byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    final InputStream is = new NotFullyAvailableAtTheTimeInputStream(data, 5);

    final int result = testedInstance.fromInputStream(is, 15);
    Assert.assertEquals(result, data.length);
    Assert.assertEquals(
        (Integer) getFieldValue(testedInstance, "size"), Integer.valueOf(data.length));

    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    assertArrayEquals(source, data);
  }

  @Test
  public void testReadFromInputStreamWithWaitSizeTooSmallLimit() throws Exception {
    final byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    final byte[] expected = Arrays.copyOf(data, 8);
    final InputStream is = new NotFullyAvailableAtTheTimeInputStream(data, 5);

    final int result = testedInstance.fromInputStream(is, 8);
    Assert.assertEquals(result, expected.length);
    Assert.assertEquals(
        (Integer) getFieldValue(testedInstance, "size"), Integer.valueOf(expected.length));

    final byte[] source = (byte[]) getFieldValue(testedInstance, "source");
    assertArrayEquals(source, expected);
  }

  private static final class NotFullyAvailableAtTheTimeInputStream extends InputStream {

    private final byte[] data;
    private int pos = -1;
    private int interrupt;

    private NotFullyAvailableAtTheTimeInputStream(byte[] data, int interrupt) {
      this.data = data;
      this.interrupt = interrupt;
      assert interrupt < data.length;
    }

    @Override
    public int read() throws IOException {
      pos++;
      if (pos < interrupt) {
        return data[pos];
      } else if (pos == interrupt) {
        return -1;
      } else if (pos <= data.length) {
        return data[pos - 1];
      } else {
        return -1;
      }
    }
  }
}
