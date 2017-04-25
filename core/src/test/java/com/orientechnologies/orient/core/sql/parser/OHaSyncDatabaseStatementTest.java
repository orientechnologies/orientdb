package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OHaSyncDatabaseStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("HA SYNC DATABASE");
    checkRightSyntax("ha sync database");

    checkRightSyntax("ha sync database -force");
    checkRightSyntax("ha sync database -full");
    checkRightSyntax("ha sync database -force -full");

    checkWrongSyntax("HA SYNC");
    checkWrongSyntax("HA SYNC DATABASE foo");
  }

}
