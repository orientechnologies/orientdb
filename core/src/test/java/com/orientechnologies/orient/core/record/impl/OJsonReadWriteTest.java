package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by tglman on 25/01/16.
 */
public class OJsonReadWriteTest {

  @Test
  public void testCustomField(){
    ODocument doc = new ODocument();
    doc.field("test",String.class, OType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    ODocument doc1 = new ODocument();
    doc1.fromJSON(json);
    assertEquals(doc.field("test"),doc1.field("test"));


  }
}
