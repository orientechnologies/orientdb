package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateFunctionStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE FUNCTION test \"print('\\nTest!')\"");
    checkRightSyntax("CREATE FUNCTION test \"return a + b;\" PARAMETERS [a,b]");
    checkRightSyntax(
        "CREATE FUNCTION allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" LANGUAGE SQL");
    checkRightSyntax(
        "create function allUsersButAdmin \"SELECT FROM ouser WHERE name <> 'admin'\" parameters [a,b] language SQL");
  }
}
