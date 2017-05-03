package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 05/10/15.
 */
public class ODocumentSerializerNetworkTestV37 extends ODocumentSchemalessBinarySerializationTest {



  @Before
  public void createSerializer() {
    serializer = new ORecordSerializerNetworkV37();
  }


  @Test
  public void testPartialNotFound() {
    ODocument document = new ODocument();
    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] { "foo" });

    assertEquals(document.field("name"), extr.<Object>field("name"));
    assertEquals(document.<Object>field("age"), extr.field("age"));
    assertEquals(document.<Object>field("youngAge"), extr.field("youngAge"));
    assertEquals(document.<Object>field("oldAge"), extr.field("oldAge"));

  }




}
