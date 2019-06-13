package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class OPropertyAccessTest {

  @Test
  public void testNotAccessible() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertEquals("one value", doc.getProperty("name"));
    assertEquals("one value", doc.field("name"));
    assertTrue(doc.containsField("name"));
    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertNull(doc.getProperty("name"));
    assertNull(doc.field("name"));
    assertFalse(doc.containsField("name"));
    assertNull(doc.fieldType("name"));
  }

  @Test
  public void testNotAccessibleAfterConvert() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    ODocument doc1 = new ODocument();
    doc1.fromStream(doc.toStream());
    assertEquals("one value", doc1.getProperty("name"));
    assertEquals("one value", doc1.field("name"));
    assertTrue(doc1.containsField("name"));
    assertEquals(OType.STRING, doc1.fieldType("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc1, new OPropertyAccess(toHide));
    assertNull(doc1.getProperty("name"));
    assertNull(doc1.field("name"));
    assertFalse(doc1.containsField("name"));
    assertNull(doc1.fieldType("name"));
  }

  @Test
  public void testNotAccessiblePropertyListing() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertArrayEquals(new String[] { "name" }, doc.fieldNames());
    assertArrayEquals(new String[] { "one value" }, doc.fieldValues());
    for (Map.Entry<String, Object> e : doc) {
      assertEquals("name", e.getKey());
    }

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertArrayEquals(new String[] {}, doc.fieldNames());
    assertArrayEquals(new String[] {}, doc.fieldValues());
    for (Map.Entry<String, Object> e : doc) {
      assertNotEquals("name", e.getKey());
    }
  }
}
