package com.orientechnologies.orient.core.id;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ORecordIdTest {

  @Test
  public void testIsString() {
    assertTrue(ORecordId.isA("#10:20"));
    assertTrue(ORecordId.isA("#-1:20"));
    assertTrue(ORecordId.isA("#-1:-1"));
    assertTrue(ORecordId.isA(" #10:20"));
    assertTrue(ORecordId.isA(" #10:20 "));
    assertTrue(ORecordId.isA("#10:20 "));
    assertFalse(ORecordId.isA("0:0"));
    assertFalse(ORecordId.isA("abc"));
    assertFalse(ORecordId.isA("#10"));
  }
}
