package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropSystemUserStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("DROP SYSTEM USER test ");
    checkRightSyntaxServer("DROP SYSTEM USER ?");
    checkRightSyntaxServer("DROP SYSTEM USER :foo");
    checkWrongSyntaxServer("DROP SYSTEM USER");
  }
}
