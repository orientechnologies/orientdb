package com.orientechnologies.orient.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by luigidellaquila on 26/04/16.
 */
public class OIdentifierTest {

  @Test
  public void testBackTickQuoted() {
    OIdentifier identifier = new OIdentifier(-1);
    identifier.setValue("foo\\`bar");
    Assert.assertEquals(identifier.getStringValue(), "foo`bar");
    Assert.assertEquals(identifier.getValue(), "foo\\`bar");

    identifier.setStringValue("foo`bar");
    Assert.assertEquals(identifier.getStringValue(), "foo`bar");
    Assert.assertEquals(identifier.getValue(), "foo\\`bar");
  }
}
