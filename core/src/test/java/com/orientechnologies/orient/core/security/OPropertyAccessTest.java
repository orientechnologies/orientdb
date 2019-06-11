package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OPropertyAccessTest {

  @Test
  public void testNotAccessible() {
    ODocument doc = new ODocument();
    doc.setProperty("name", "one value");
    assertEquals("one value", doc.getProperty("name"));
    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(toHide));
    assertNull(doc.getProperty("name"));
  }
}
