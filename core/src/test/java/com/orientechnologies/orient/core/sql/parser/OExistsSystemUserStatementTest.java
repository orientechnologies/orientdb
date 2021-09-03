package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OExistsSystemUserStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("EXISTS SYSTEM USER test ");
    checkRightSyntaxServer("EXISTS SYSTEM USER ?");
    checkRightSyntaxServer("EXISTS SYSTEM USER :foo");
    checkWrongSyntaxServer("EXISTS SYSTEM USER");
  }
}
