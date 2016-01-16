package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test
public class OCompositeKeyTest {

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

  public void testEqualNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertFalse(compositeKey.equals(null));
  }

  public void testEqualSame() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertTrue(compositeKey.equals(compositeKey));
  }

  public void testEqualDiffClass() {
    final OCompositeKey compositeKey = new OCompositeKey();
    assertFalse(compositeKey.equals("1"));
  }

  public void testAddKeyComparable() {
    final OCompositeKey compositeKey = new OCompositeKey();

    compositeKey.addKey("a");

    assertEquals(compositeKey.getKeys().size(), 1);
    assertTrue(compositeKey.getKeys().contains("a"));
  }

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

  public void testCompareToSame() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
  }

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

  public void testCompareToGT() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), 1);
  }

  public void testCompareToLT() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final OCompositeKey anotherCompositeKey = new OCompositeKey();

    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey.compareTo(anotherCompositeKey), -1);
  }

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

  public void testNativeBinarySerializationCompositeKeyNull() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    int len = OCompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);
    byte[] data = new byte[len];
    OCompositeKeySerializer.INSTANCE.serializeNativeObject(compositeKeyOne, data, 0);

    final OCompositeKey compositeKeyTwo = OCompositeKeySerializer.INSTANCE.deserializeNativeObject(data, 0);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
  }

  public void testDirectMemoryBinarySerializationCompositeKeyNull() {
    final OCompositeKey compositeKeyOne = new OCompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    int len = OCompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);
    ODirectMemoryPointer directMemoryPointer = ODirectMemoryPointerFactory.instance().createPointer(len);

    OCompositeKeySerializer.INSTANCE.serializeInDirectMemoryObject(compositeKeyOne, directMemoryPointer, 0);

    final OCompositeKey compositeKeyTwo = OCompositeKeySerializer.INSTANCE.deserializeFromDirectMemoryObject(directMemoryPointer, 0);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
    directMemoryPointer.free();
  }
}
