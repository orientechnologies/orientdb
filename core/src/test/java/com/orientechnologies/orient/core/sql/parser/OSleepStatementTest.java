package com.orientechnologies.orient.core.sql.parser;

public class OSleepStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("SLEEP 100");

    checkWrongSyntax("SLEEP");
    checkWrongSyntax("SLEEP 1 3 5");
    checkWrongSyntax("SLEEP 1.5");
    checkWrongSyntax("SLEEP 1,5");
  }
}
