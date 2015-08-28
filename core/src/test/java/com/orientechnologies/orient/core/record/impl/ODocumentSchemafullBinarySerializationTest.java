package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.testng.annotations.Test;

@Test
public class ODocumentSchemafullBinarySerializationTest extends ODocumentSchemafullSerializationTest{

  public ODocumentSchemafullBinarySerializationTest() {
    super(new ORecordSerializerBinary());
  }


}
