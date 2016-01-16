package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;

/**
 * Created by tglman on 05/10/15.
 */
public class ODocumentSerializerNetworkTest extends ODocumentSchemalessBinarySerializationTest {


  public ODocumentSerializerNetworkTest() {
    serializer = new ORecordSerializerNetwork();
  }

}
