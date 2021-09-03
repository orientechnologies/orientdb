package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropViewStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP VIEW Foo");
    checkRightSyntax("drop view Foo");
    checkRightSyntax("DROP VIEW `Foo bar`");

    checkWrongSyntax("drop VIEW Foo UNSAFE ");
    checkWrongSyntax("drop view Foo bar");
  }

  @Test
  public void testIfExists() {
    checkRightSyntax("DROP VIEW Foo if exists");
    checkRightSyntax("DROP VIEW Foo IF EXISTS");

    checkWrongSyntax("drop view Foo if");
    checkWrongSyntax("drop view Foo if exists lkj");
    checkWrongSyntax("drop view Foo if lkj");
  }
}
