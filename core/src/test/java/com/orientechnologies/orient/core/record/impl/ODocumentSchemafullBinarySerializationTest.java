package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;

public class ODocumentSchemafullBinarySerializationTest
    extends ODocumentSchemafullSerializationTest {

  public ODocumentSchemafullBinarySerializationTest() {
    super(new ORecordSerializerBinary());
  }
}
