package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.Orient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/19/2015
 */
@Test
public class OWALPageChangesPortionTest {

  @BeforeMethod
  public void before() {
    Orient.instance();
  }

  public void testSingleLongValueInStartChunk() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setLongValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 64), 42);

  }

  public void testSingleLongValuesInMiddleOfChunk() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setLongValue(pointer, 42, 60);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 60), 42);

  }

  public void testSingleIntValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setIntValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getIntValue(pointer, 64), 42);

  }

  public void testSingleShortValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setShortValue(pointer, (short) 42, 64);
    Assert.assertEquals(changesCollector.getShortValue(pointer, 64), 42);

  }

  public void testSingleByteValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setByteValue(pointer, (byte) 42, 64);
    Assert.assertEquals(changesCollector.getByteValue(pointer, 64), 42);

  }

  public void testMoveData() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] values = new byte[] { 1, 2, 3, 4 };

    changesCollector.setBinaryValue(pointer, values, 64);
    changesCollector.moveData(pointer, 64, 74, 4);
    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 64, 4), values);

  }


  public void testBinaryValueTwoChunksFromStart() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 64);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 64, 128), changes);

  }

  public void testBinaryValueTwoChunksInMiddle() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 32, 128), changes);

  }


  public void testBinaryValueTwoChunksTwoPortionsInMiddle() {
    byte[] data = new byte[65536];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(65536);
    byte[] changes = new byte[1024];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 1000);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 1000, 1024), changes);

  }


  public void testSimpleApplyChanges() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 32, 128), changes);

    changesCollector.applyChanges(pointer);
    byte[] res = new byte[128];
    pointer.position(32);
    pointer.get(res);
    Assert.assertEquals(res, changes);
  }


  public void testSerializationAndRestore() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 32, 128), changes);

    ByteBuffer newBuffer = ByteBuffer.wrap(new byte[1024]);
    newBuffer.put(pointer.array());

    int size = changesCollector.serializedSize();
    byte[] content = new byte[size];
    changesCollector.toStream(0, content);
    OWALPageChangesPortion changesCollector1 = new OWALPageChangesPortion(1024);
    changesCollector1.fromStream(0, content);
    changesCollector1.applyChanges(newBuffer);

    Assert.assertEquals(pointer.compareTo(newBuffer), 0);

  }

  public void testEmptyChanges() {
    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    int size = changesCollector.serializedSize();
    byte[] bytes = new byte[size];
    changesCollector.toStream(0, bytes);
    OWALPageChangesPortion changesCollector1 = new OWALPageChangesPortion(1024);
    changesCollector1.fromStream(0, bytes);

    Assert.assertEquals(size, changesCollector1.serializedSize());

  }

  public void testReadNoChanges() {
    byte[] data = new byte[1024];
    data[0] = 1;
    data[1] = 2;
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] bytes = changesCollector.getBinaryValue(pointer, 0, 2);
    Assert.assertEquals(bytes[0], 1);
    Assert.assertEquals(bytes[1], 2);
  }

  public void testGetCrossChanges() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);

    byte[] changes = new byte[32];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);
    changesCollector.setBinaryValue(pointer, changes, 128);

    byte[] content = changesCollector.getBinaryValue(pointer, 32, 128);
    byte[] expected = new byte[128];
    System.arraycopy(changes, 0, expected, 0, 32);
    System.arraycopy(changes, 0, expected, 96, 32);
    Assert.assertEquals(content, expected);
  }

  public void testMultiPortionReadIfFirstPortionIsNotChanged() {
    final byte[] data = new byte[OWALPageChangesPortion.PORTION_BYTES * 4];
    final ByteBuffer pointer = ByteBuffer.wrap(data);
    final OWALPageChangesPortion changes = new OWALPageChangesPortion(data.length);

    final byte[] smallChange = new byte[32];
    Random random = new Random();
    random.nextBytes(smallChange);

    changes.setBinaryValue(pointer, smallChange, OWALPageChangesPortion.PORTION_BYTES + 37);

    final byte[] actual = changes.getBinaryValue(pointer, 0, OWALPageChangesPortion.PORTION_BYTES * 2);

    final byte[] expected = new byte[OWALPageChangesPortion.PORTION_BYTES * 2];
    System.arraycopy(smallChange, 0, expected, OWALPageChangesPortion.PORTION_BYTES + 37, smallChange.length);

    Assert.assertEquals(actual, expected);
  }

}
