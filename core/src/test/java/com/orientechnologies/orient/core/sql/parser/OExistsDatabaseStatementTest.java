package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OExistsDatabaseStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("EXISTS DATABASE foo");
    checkWrongSyntax("EXISTS DATABASE");
  }
}
