package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OReturnStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("RETURN");
    checkRightSyntax("return");
    checkRightSyntax("RETURN 1");
    checkRightSyntax("RETURN [1, 3]");
    checkRightSyntax("RETURN [$a, $b, $c]");
    checkRightSyntax("RETURN (select from foo)");

    checkWrongSyntax("return foo bar");
  }
}
