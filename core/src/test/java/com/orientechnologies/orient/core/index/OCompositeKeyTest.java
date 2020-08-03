package com.orientechnologies.orient.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class OCompositeKeyTest {

  @Test
  public void testEqualSameKeys() {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertTrue(compositeKey.equals(anotherCompositeKey));
    assertTrue(compositeKey.hashCode() == anotherCompositeKey.hashCode());
  }

  @Test
  public void testEqualNotSameKeys() {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");
    anotherCompositeKey.addKey("c");

    assertFalse(compositeKey.equals(anotherCompositeKey));
  }

  @Test
  public void testEqualNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertFalse(compositeKey.equals(null));
  }

  @Test
  public void testEqualSame() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertTrue(compositeKey.equals(compositeKey));
  }

  @Test
  public void testEqualDiffClass() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertFalse(compositeKey.equals("1"));
  }

  @Test
  public void testAddKeyComparable() {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey("a");

    assertEquals(compositeKey.getKeys().size(), 1);
    assertTrue(compositeKey.getKeys().contains("a"));
  }

  @Test
  public void testAddKeyComposite() {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey("a");

    final OCompositeKey compositeKeyToAdd = new OCompositeKey();
    compositeKeyToAdd.addKey("a");
    compositeKeyToAdd.addKey("b");

    compositeKey.addKey(compositeKeyToAdd);

    assertEquals(compositeKey.getKeys().size(), 3);
    assertTrue(compositeKey.getKeys().contains("a"));
    assertTrue(compositeKey.getKeys().contains("b"));
  }

  @Test
  public void testCompareToSame() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
  }

  @Test
  public void testCompareToPartiallyOneCase() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");
    anotherCompositeKey.addKey("c");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
  }

  @Test
  public void testCompareToPartiallySecondCase() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");
    compositeKey.addKey("c");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
  }

  @Test
  public void testCompareToGT() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 1);
  }

  @Test
  public void testCompareToLT() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();

    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), -1);
  }

  @Test
  public void testCompareStringsToLT() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("name4");
    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("name5");
    assertEquals(compositeKey.compareTo(anotherCompositeKey), -1);
  }

  @Test
  public void testCompareToSymmetryOne() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(2);

    final OCompositeKey compositeKeyTwo = new OCompositeKey();
    compositeKeyTwo.addKey(1);
    compositeKeyTwo.addKey(3);
    compositeKeyTwo.addKey(1);

    assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), -1);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), 1);
  }

  @Test
  public void testCompareToSymmetryTwo() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(2);

    final OCompositeKey compositeKeyTwo = new OCompositeKey();
    compositeKeyTwo.addKey(1);
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(3);

    assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), 0);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), 0);
  }

  @Test
  public void testCompareNullAtTheEnd() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(2);

    final OCompositeKey compositeKeyTwo = new OCompositeKey();
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(null);

    final OCompositeKey compositeKeyThree = new OCompositeKey();
    compositeKeyThree.addKey(2);
    compositeKeyThree.addKey(null);

    assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), 1);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), -1);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyThree), 0);
  }

  @Test
  public void testCompareNullAtTheMiddle() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(3);

    final OCompositeKey compositeKeyTwo = new OCompositeKey();
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(null);
    compositeKeyTwo.addKey(3);

    final OCompositeKey compositeKeyThree = new OCompositeKey();
    compositeKeyThree.addKey(2);
    compositeKeyThree.addKey(null);
    compositeKeyThree.addKey(3);

    assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), 1);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), -1);
    assertEquals(compositeKeyTwo.compareTo(compositeKeyThree), 0);
  }

  @Test
  public void testDocumentSerializationCompositeKeyNull() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    ODocument document = compositeKeyOne.toDocument();

    final OCompositeKey compositeKeyTwo = new OCompositeKey();
    compositeKeyTwo.fromDocument(document);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
  }

  @Test
  public void testNativeBinarySerializationCompositeKeyNull() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    int len = OCompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);
    byte[] data = new byte[len];
    OCompositeKeySerializer.INSTANCE.serializeNativeObject(compositeKeyOne, data, 0);

    final OCompositeKey compositeKeyTwo =
        OCompositeKeySerializer.INSTANCE.deserializeNativeObject(data, 0);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
  }

  @Test
  public void testByteBufferBinarySerializationCompositeKeyNull() {
    final int serializationOffset = 5;

    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    final int len = OCompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    OCompositeKeySerializer.INSTANCE.serializeInByteBufferObject(compositeKeyOne, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    assertEquals(OCompositeKeySerializer.INSTANCE.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    final OCompositeKey compositeKeyTwo =
        OCompositeKeySerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);

    assertEquals(buffer.position() - serializationOffset, len);
  }

  @Test
  public void testWALChangesBinarySerializationCompositeKeyNull() {
    final int serializationOffset = 5;

    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1);
    compositeKey.addKey(null);
    compositeKey.addKey(2);

    final int len = OCompositeKeySerializer.INSTANCE.getObjectSize(compositeKey);
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(len + serializationOffset).order(ByteOrder.nativeOrder());
    final byte[] data = new byte[len];

    OCompositeKeySerializer.INSTANCE.serializeNativeObject(compositeKey, data, 0);
    final OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    assertEquals(
        OCompositeKeySerializer.INSTANCE.getObjectSizeInByteBuffer(
            buffer, walChanges, serializationOffset),
        len);
    assertEquals(
        OCompositeKeySerializer.INSTANCE.deserializeFromByteBufferObject(
            buffer, walChanges, serializationOffset),
        compositeKey);
  }

  @Test
  public void testNetworkSerialization() throws IOException {
    String k1 = "key";
    OCompositeKey k2 = new OCompositeKey(null, new OCompositeKey("user1", 12.5));
    OCompositeKey compositeKey = new OCompositeKey(k1, k2);
    ORecordSerializerNetworkV37 serializer = ORecordSerializerNetworkV37.INSTANCE;
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outStream);
    compositeKey.toStream(serializer, out);
    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInputStream in = new DataInputStream(inStream);
    OCompositeKey deserializedCompositeKey = new OCompositeKey();
    deserializedCompositeKey.fromStream(serializer, in);
    assertEquals(compositeKey, deserializedCompositeKey);
  }
}
