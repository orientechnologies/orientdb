package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.Orient;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 8/19/2015
 */
public class OWALPageV2ChangesPortionTest {

  @Before
  public void before() {
    Orient.instance();
  }

  @Test
  public void testSingleLongValueInStartChunk() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    pointer.putLong(64, 31);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setLongValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 64), 42);

    changesCollector.applyChanges(pointer);
    Assert.assertEquals(42, pointer.getLong(64));
  }

  @Test
  public void testSingleLongValuesInMiddleOfChunk() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    pointer.putLong(60, 31);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setLongValue(pointer, 42, 60);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 60), 42);

    changesCollector.applyChanges(pointer);
    Assert.assertEquals(42, pointer.getLong(60));
  }

  @Test
  public void testSingleIntValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    pointer.putLong(64, 31);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setIntValue(pointer, 42, 64);
    Assert.assertEquals(changesCollector.getIntValue(pointer, 64), 42);

    changesCollector.applyChanges(pointer);
    Assert.assertEquals(42, pointer.getInt(64));
  }

  @Test
  public void testSingleShortValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    pointer.putShort(64, (short) 31);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setShortValue(pointer, (short) 42, 64);
    Assert.assertEquals(changesCollector.getShortValue(pointer, 64), 42);

    changesCollector.applyChanges(pointer);
    Assert.assertEquals((short) 42, pointer.getShort(64));
  }

  @Test
  public void testSingleByteValue() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    pointer.put(64, (byte) 31);

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    changesCollector.setByteValue(pointer, (byte) 42, 64);
    Assert.assertEquals(changesCollector.getByteValue(pointer, 64), 42);

    changesCollector.applyChanges(pointer);
    Assert.assertEquals((byte) 42, pointer.get(64));
  }

  @Test
  public void testMoveData() {
    byte[] data = new byte[1024];
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    pointer.position(64);
    pointer.put(new byte[] {11, 12, 13, 14});

    pointer.position(74);
    pointer.put(new byte[] {21, 22, 23, 24});

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] values = new byte[] {1, 2, 3, 4};

    changesCollector.setBinaryValue(pointer, values, 64);
    changesCollector.moveData(pointer, 64, 74, 4);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 64, 4)).isEqualTo(values);
    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 74, 4)).isEqualTo(values);

    changesCollector.applyChanges(pointer);

    byte[] result = new byte[4];
    pointer.position(64);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(values);

    pointer.position(74);
    pointer.get(result);
    Assertions.assertThat(result).isEqualTo(values);
  }

  @Test
  public void testBinaryValueTwoChunksFromStart() {
    final byte[] originalData = new byte[1024];

    Random random = new Random();
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);

    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 64);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 64, 128)).isEqualTo(changes);

    changesCollector.applyChanges(pointer);
    byte[] result = new byte[128];
    pointer.position(64);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(changes);
  }

  @Test
  public void testBinaryValueTwoChunksInMiddle() {
    final byte[] originalData = new byte[1024];

    Random random = new Random();
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);

    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);
    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 32, 128)).isEqualTo(changes);

    changesCollector.applyChanges(pointer);

    byte[] result = new byte[128];
    pointer.position(32);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(changes);
  }

  @Test
  public void testBinaryValueTwoChunksTwoPortionsInMiddle() {
    Random random = new Random();

    byte[] originalData = new byte[65536];
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(65536);
    byte[] changes = new byte[1024];

    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 1000);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 1000, 1024)).isEqualTo(changes);

    changesCollector.applyChanges(pointer);

    byte[] result = new byte[1024];

    pointer.position(1000);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(changes);
  }

  @Test
  public void testSimpleApplyChanges() {
    Random random = new Random();

    byte[] originalData = new byte[1024];
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);
    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    byte[] changes = new byte[128];

    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 32, 128)).isEqualTo(changes);

    changesCollector.applyChanges(pointer);
    byte[] res = new byte[128];
    pointer.position(32);
    pointer.get(res);

    Assertions.assertThat(res).isEqualTo(changes);
  }

  @Test
  public void testSerializationAndRestore() {
    Random random = new Random();
    byte[] originalData = new byte[1024];
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);

    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);

    byte[] changes = new byte[128];
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 32, 128)).isEqualTo(changes);
    changesCollector.applyChanges(pointer);

    ByteBuffer newBuffer =
        ByteBuffer.wrap(Arrays.copyOf(originalData, originalData.length))
            .order(ByteOrder.nativeOrder());

    int size = changesCollector.serializedSize();
    byte[] content = new byte[size];
    changesCollector.toStream(0, content);

    OWALPageChangesPortion changesCollectorRestored = new OWALPageChangesPortion(1024);
    changesCollectorRestored.fromStream(0, content);
    changesCollectorRestored.applyChanges(newBuffer);

    newBuffer.position(0);
    pointer.position(0);
    Assert.assertEquals(pointer.compareTo(newBuffer), 0);
  }

  @Test
  public void testSerializationAndRestoreFromBuffer() {
    Random random = new Random();
    byte[] originalData = new byte[1024];
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);

    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);

    byte[] changes = new byte[128];
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);

    Assertions.assertThat(changesCollector.getBinaryValue(pointer, 32, 128)).isEqualTo(changes);
    changesCollector.applyChanges(pointer);

    ByteBuffer newBuffer =
        ByteBuffer.wrap(Arrays.copyOf(originalData, originalData.length))
            .order(ByteOrder.nativeOrder());

    int size = changesCollector.serializedSize();
    byte[] content = new byte[size];
    changesCollector.toStream(0, content);

    OWALPageChangesPortion changesCollectorRestored = new OWALPageChangesPortion(1024);
    changesCollectorRestored.fromStream(ByteBuffer.wrap(content).order(ByteOrder.nativeOrder()));
    changesCollectorRestored.applyChanges(newBuffer);

    newBuffer.position(0);
    pointer.position(0);
    Assert.assertEquals(pointer.compareTo(newBuffer), 0);
  }

  @Test
  public void testEmptyChanges() {
    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);
    int size = changesCollector.serializedSize();
    byte[] bytes = new byte[size];
    changesCollector.toStream(0, bytes);
    OWALPageChangesPortion changesCollectorRestored = new OWALPageChangesPortion(1024);
    changesCollectorRestored.fromStream(0, bytes);

    Assert.assertEquals(size, changesCollectorRestored.serializedSize());
  }

  @Test
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

  @Test
  public void testGetCrossChanges() {
    Random random = new Random();

    byte[] originalData = new byte[1024];
    random.nextBytes(originalData);

    byte[] data = Arrays.copyOf(originalData, originalData.length);

    ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

    OWALPageChangesPortion changesCollector = new OWALPageChangesPortion(1024);

    byte[] changes = new byte[32];
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, changes, 32);
    changesCollector.setBinaryValue(pointer, changes, 128);

    byte[] content = changesCollector.getBinaryValue(pointer, 32, 128);

    byte[] expected = Arrays.copyOfRange(originalData, 32, 160);
    System.arraycopy(changes, 0, expected, 0, 32);
    System.arraycopy(changes, 0, expected, 96, 32);

    Assertions.assertThat(content).isEqualTo(expected);

    changesCollector.applyChanges(pointer);
    byte[] result = new byte[128];
    pointer.position(32);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testMultiPortionReadIfFirstPortionIsNotChanged() {
    Random random = new Random();

    final byte[] originalData = new byte[OWALPageChangesPortion.PORTION_BYTES * 4];
    random.nextBytes(originalData);

    final byte[] data = Arrays.copyOf(originalData, originalData.length);

    final ByteBuffer pointer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    final OWALPageChangesPortion changes = new OWALPageChangesPortion(data.length);

    final byte[] smallChange = new byte[32];
    random.nextBytes(smallChange);

    changes.setBinaryValue(pointer, smallChange, OWALPageChangesPortion.PORTION_BYTES + 37);

    final byte[] actual =
        changes.getBinaryValue(pointer, 0, OWALPageChangesPortion.PORTION_BYTES * 2);

    final byte[] expected =
        Arrays.copyOfRange(originalData, 0, OWALPageChangesPortion.PORTION_BYTES * 2);
    System.arraycopy(
        smallChange, 0, expected, OWALPageChangesPortion.PORTION_BYTES + 37, smallChange.length);

    Assertions.assertThat(actual).isEqualTo(expected);

    changes.applyChanges(pointer);

    byte[] result = new byte[OWALPageChangesPortion.PORTION_BYTES * 2];
    pointer.position(0);
    pointer.get(result);

    Assertions.assertThat(result).isEqualTo(expected);
  }
}
