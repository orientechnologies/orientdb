package com.orientechnologies.orient.core.security;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

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
    assertNull(doc.field("name", OType.STRING));
    assertNull(doc.field("name", String.class));
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
    assertArrayEquals(new String[] {"name"}, doc.fieldNames());
    assertArrayEquals(new String[] {"one value"}, doc.fieldValues());
    assertEquals(new HashSet<String>(Arrays.asList("name")), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertEquals("name", e.getKey());
    }

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertArrayEquals(new String[] {}, doc.fieldNames());
    assertArrayEquals(new String[] {}, doc.fieldValues());
    assertEquals(new HashSet<String>(), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertNotEquals("name", e.getKey());
    }
  }

  @Test
  public void testNotAccessiblePropertyListingSer() {
    ODocument docPre = new ODocument();
    docPre.setProperty("name", "one value");
    assertArrayEquals(new String[] {"name"}, docPre.fieldNames());
    assertArrayEquals(new String[] {"one value"}, docPre.fieldValues());
    assertEquals(new HashSet<String>(Arrays.asList("name")), docPre.getPropertyNames());
    for (Map.Entry<String, Object> e : docPre) {
      assertEquals("name", e.getKey());
    }

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocument doc = new ODocument();
    doc.fromStream(docPre.toStream());
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertArrayEquals(new String[] {}, doc.fieldNames());
    assertArrayEquals(new String[] {}, doc.fieldValues());
    assertEquals(new HashSet<String>(), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertNotEquals("name", e.getKey());
    }
  }

  @Test
  public void testJsonSerialization() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertTrue(doc.toJSON().contains("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertFalse(doc.toJSON().contains("name"));
  }

  @Test
  public void testToMap() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertTrue(doc.toMap().containsKey("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertFalse(doc.toMap().containsKey("name"));
  }

  @Test
  public void testStringSerialization() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertTrue(doc.toString().contains("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertFalse(doc.toString().contains("name"));
  }
}
