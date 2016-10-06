package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OHaRemoveServerStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("HA REMOVE SERVER foo");
    checkRightSyntax("ha remove server foo");

    checkWrongSyntax("HA REMOVE");
    checkWrongSyntax("HA REMOVE SERVER");
    checkWrongSyntax("HA REMOVE SERVER foo bar");
  }

}
