package com.orientechnologies.orient.core.record.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

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
  public void testReadSimple() {
    BytesContainer bytesContainer = new BytesContainer();
    bytesContainer.read((short) 100);
    assertEquals(bytesContainer.offset, 100);
  }

}
