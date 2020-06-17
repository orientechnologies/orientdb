package com.orientechnologies.common.serialization;

import java.nio.ByteOrder;
import org.junit.Assert;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 21.05.13
 */
public abstract class AbstractConverterTest {
  protected OBinaryConverter converter;

  public void testPutIntBigEndian() {
    int value = 0xFE23A067;

    byte[] result = new byte[4];
    converter.putInt(result, 0, value, ByteOrder.BIG_ENDIAN);

    Assert.assertEquals(result, new byte[] {(byte) 0xFE, 0x23, (byte) 0xA0, 0x67});

    Assert.assertEquals(converter.getInt(result, 0, ByteOrder.BIG_ENDIAN), value);
  }

  public void testPutIntLittleEndian() {
    int value = 0xFE23A067;

    byte[] result = new byte[4];
    converter.putInt(result, 0, value, ByteOrder.LITTLE_ENDIAN);

    Assert.assertEquals(result, new byte[] {0x67, (byte) 0xA0, 0x23, (byte) 0xFE});

    Assert.assertEquals(converter.getInt(result, 0, ByteOrder.LITTLE_ENDIAN), value);
  }

  public void testPutLongBigEndian() {
    long value = 0xFE23A067ED890C14L;

    byte[] result = new byte[8];
    converter.putLong(result, 0, value, ByteOrder.BIG_ENDIAN);

    Assert.assertEquals(
        result,
        new byte[] {(byte) 0xFE, 0x23, (byte) 0xA0, 0x67, (byte) 0xED, (byte) 0x89, 0x0C, 0x14});
    Assert.assertEquals(converter.getLong(result, 0, ByteOrder.BIG_ENDIAN), value);
  }

  public void testPutLongLittleEndian() {
    long value = 0xFE23A067ED890C14L;

    byte[] result = new byte[8];
    converter.putLong(result, 0, value, ByteOrder.LITTLE_ENDIAN);

    Assert.assertEquals(
        result,
        new byte[] {0x14, 0x0C, (byte) 0x89, (byte) 0xED, 0x67, (byte) 0xA0, 0x23, (byte) 0xFE});

    Assert.assertEquals(converter.getLong(result, 0, ByteOrder.LITTLE_ENDIAN), value);
  }

  public void testPutShortBigEndian() {
    short value = (short) 0xA028;
    byte[] result = new byte[2];

    converter.putShort(result, 0, value, ByteOrder.BIG_ENDIAN);

    Assert.assertEquals(result, new byte[] {(byte) 0xA0, 0x28});
    Assert.assertEquals(converter.getShort(result, 0, ByteOrder.BIG_ENDIAN), value);
  }

  public void testPutShortLittleEndian() {
    short value = (short) 0xA028;
    byte[] result = new byte[2];

    converter.putShort(result, 0, value, ByteOrder.LITTLE_ENDIAN);

    Assert.assertEquals(result, new byte[] {0x28, (byte) 0xA0});
    Assert.assertEquals(converter.getShort(result, 0, ByteOrder.LITTLE_ENDIAN), value);
  }

  public void testPutCharBigEndian() {
    char value = (char) 0xA028;
    byte[] result = new byte[2];

    converter.putChar(result, 0, value, ByteOrder.BIG_ENDIAN);

    Assert.assertEquals(result, new byte[] {(byte) 0xA0, 0x28});
    Assert.assertEquals(converter.getChar(result, 0, ByteOrder.BIG_ENDIAN), value);
  }

  public void testPutCharLittleEndian() {
    char value = (char) 0xA028;
    byte[] result = new byte[2];

    converter.putChar(result, 0, value, ByteOrder.LITTLE_ENDIAN);

    Assert.assertEquals(result, new byte[] {0x28, (byte) 0xA0});
    Assert.assertEquals(converter.getChar(result, 0, ByteOrder.LITTLE_ENDIAN), value);
  }
}
