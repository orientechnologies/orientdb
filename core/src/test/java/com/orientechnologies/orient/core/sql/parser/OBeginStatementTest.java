package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OBeginStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("BEGIN");
    checkRightSyntax("begin");

    checkWrongSyntax("BEGIN foo ");
  }


}
