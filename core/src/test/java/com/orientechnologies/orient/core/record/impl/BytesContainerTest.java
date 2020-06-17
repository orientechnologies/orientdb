package com.orientechnologies.orient.core.record.impl;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import org.junit.Test;

public class BytesContainerTest {

  @Test
  public void testSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    assertNotNull(bytesContainer.bytes);
    assertEquals(bytesContainer.offset, 0);
  }

  @Test
  public void testReallocSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.alloc((short) 2050);
    assertTrue(bytesContainer.bytes.length > 2050);
    assertEquals(bytesContainer.offset, 2050);
  }

  @Test
  public void testBorderReallocSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.alloc((short) 1024);
    int pos = bytesContainer.alloc((short) 1);
    bytesContainer.bytes[pos] = 0;
    assertTrue(bytesContainer.bytes.length >= 1025);
    assertEquals(bytesContainer.offset, 1025);
  }

  @Test
  public void testReadSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.skip((short) 100);
    assertEquals(bytesContainer.offset, 100);
  }
}
