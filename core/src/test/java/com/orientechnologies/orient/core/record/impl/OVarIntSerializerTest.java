package com.orientechnologies.orient.core.record.impl;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;

public class OVarIntSerializerTest {

  @Test
  public void serializeZero() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSerializer.write(bytes, 0);
    bytes.offset = 0;
    assertEquals(OVarIntSerializer.read(bytes), 0l);
  }

  @Test
  public void serializeNegative() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSerializer.write(bytes, -20432343);
    bytes.offset = 0;
    assertEquals(OVarIntSerializer.read(bytes), -20432343l);
  }

  @Test
  public void serializePositive() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSerializer.write(bytes, 20432343);
    bytes.offset = 0;
    assertEquals(OVarIntSerializer.read(bytes), 20432343l);
  }

  @Test
  public void serializeCrazyPositive() {
    BytesContainer bytes = new BytesContainer();
    OVarIntSerializer.write(bytes, 16238);
    bytes.offset = 0;
    assertEquals(OVarIntSerializer.read(bytes), 16238l);
  }

  @Test
  public void serializePosition() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSerializer.write(bytes, 16238);
    assertEquals(OVarIntSerializer.read(bytes), 16238l);
  }

  @Test
  public void serializeMaxLong() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSerializer.write(bytes, Long.MAX_VALUE);
    assertEquals(OVarIntSerializer.read(bytes), Long.MAX_VALUE);
  }

  @Test
  public void serializeMinLong() {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = OVarIntSerializer.write(bytes, Long.MIN_VALUE);
    assertEquals(OVarIntSerializer.read(bytes), Long.MIN_VALUE);
  }

}
