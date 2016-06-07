package com.orientechnologies.orient.core.sql.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by luigidellaquila on 26/04/16.
 */
@Test
public class OIdentifierTest {

  public void testBackTickQuoted(){
    OIdentifier identifier = new OIdentifier(-1);
    identifier.setValue("foo\\`bar");
    Assert.assertEquals(identifier.getStringValue(), "foo`bar");
    Assert.assertEquals(identifier.getValue(), "foo\\`bar");

    identifier.setStringValue("foo`bar");
    Assert.assertEquals(identifier.getStringValue(), "foo`bar");
    Assert.assertEquals(identifier.getValue(), "foo\\`bar");
  }
}
