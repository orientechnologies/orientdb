package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryComparator;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.junit.Assert;

public abstract class AbstractComparatorTest {

  protected ODocumentSerializer serializer =
      ORecordSerializerBinary.INSTANCE.getCurrentSerializer();
  protected OBinaryComparator comparator = serializer.getComparator();

  protected void testEquals(OType sourceType) {
    OType[] numberTypes =
        new OType[] {OType.BYTE, OType.DOUBLE, OType.FLOAT, OType.SHORT, OType.INTEGER, OType.LONG};

    for (OType t : numberTypes) {
      testEquals(t, sourceType);
    }

    for (OType t : numberTypes) {
      testEquals(sourceType, t);
    }
  }

  protected void testEquals(OType sourceType, OType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(field(sourceType, 10), field(destType, 10)));
      Assert.assertFalse(comparator.isEqual(field(sourceType, 10), field(destType, 9)));
      Assert.assertFalse(comparator.isEqual(field(sourceType, 10), field(destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      throw e;
    }
  }

  protected OBinaryField field(final OType type, final Object value) {
    return field(type, value, null);
  }

  protected OBinaryField field(final OType type, final Object value, OCollate collate) {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = serializer.serializeValue(bytes, value, type, null, null, null);
    return new OBinaryField(null, type, bytes, collate);
  }
}
