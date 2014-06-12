package com.orientechnologies.orient.core.record.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;

@Test
public class ODocumentSchemalessBinarySerializationTest {

  protected ORecordSerializer serializer;

  public ODocumentSchemalessBinarySerializationTest() {
    serializer = new ORecordSerializerBinary();
  }

  @Test
  public void testSimpleSerialization() {
    ODocument document = new ODocument();

    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.field("heigth", 12.5f);
    document.field("bitHeigth", 12.5d);
    document.field("class", (byte) 'C');
    document.field("character", 'C');
    document.field("alive", true);
    document.field("date", new Date());
    document.field("recordId", new ORecordId(10, new OClusterPositionLong(10)));

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("name"), document.field("name"));
    assertEquals(extr.field("age"), document.field("age"));
    assertEquals(extr.field("youngAge"), document.field("youngAge"));
    assertEquals(extr.field("oldAge"), document.field("oldAge"));
    assertEquals(extr.field("heigth"), document.field("heigth"));
    assertEquals(extr.field("bitHeigth"), document.field("bitHeigth"));
    assertEquals(extr.field("class"), document.field("class"));
    // TODO fix char management issue:#2427
    // assertEquals(document.field("character"), extr.field("character"));
    assertEquals(extr.field("alive"), document.field("alive"));
    assertEquals(extr.field("date"), document.field("date"));
    // assertEquals(extr.field("recordId"), document.field("recordId"));

  }

}
