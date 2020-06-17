package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OLetStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("let foo = select from foo where name = 'bar'");
    checkRightSyntax("LET foo = select from foo where name = 'bar'");
    checkRightSyntax("LET foo = select from foo where name = 'bar';");
    checkRightSyntax("LET $foo = select from foo where name = 'bar'");
    checkRightSyntax("LET $foo = (select from foo where name = 'bar')");
    checkRightSyntax("LET $foo = bar");
    checkRightSyntax("LET $foo = 1 + 2 + 3");
    checkRightSyntax("LET $foo = [1, 3, 5]");

    checkRightSyntax("LET a = create vertex Test content {\"id\": \"12345678\"}");

    checkWrongSyntax("LET ");
    checkWrongSyntax("LET foo");
  }
}
