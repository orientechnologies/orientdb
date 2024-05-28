package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import org.junit.Assert;
import org.junit.Test;

public class TestReaderDropClass extends BaseMemoryDatabase {
  @Test
  public void testReaderDropClass() {
    db.getMetadata().getSchema().createClass("Test");
    reOpen("reader", "reader");
    try {
      db.getMetadata().getSchema().dropClass("Test");
      Assert.fail("reader should not be able to drop a class");
    } catch (OSecurityAccessException ex) {
    }
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("Test"));
  }
}
