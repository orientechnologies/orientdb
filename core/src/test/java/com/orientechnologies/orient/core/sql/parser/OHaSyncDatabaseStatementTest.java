package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OHaSyncDatabaseStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA SYNC DATABASE");
    checkRightSyntax("ha sync database");
    checkRightSyntax("HA SYNC DATABASE -force");
    checkRightSyntax("HA SYNC DATABASE -full");

    checkWrongSyntax("HA SYNC DATABASE foo");
  }
}
