package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropPropertyStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP PROPERTY Foo.bar");
    checkRightSyntax("drop property Foo.bar");
    checkRightSyntax("drop property `Foo bar`.`bar baz`");
    checkRightSyntax("drop property Foo.bar force");
    checkRightSyntax("drop property Foo.bar FORCE");

    checkWrongSyntax("drop property foo");
    checkWrongSyntax("drop property foo.bar.baz");
    checkWrongSyntax("drop property foo.bar foo");
  }

  @Test
  public void testIfExists() {
    checkRightSyntax("DROP PROPERTY Foo.bar if exists");
    checkRightSyntax("DROP PROPERTY Foo.bar IF EXISTS");
    checkRightSyntax("DROP PROPERTY Foo.bar if exists force");

    checkWrongSyntax("DROP PROPERTY Foo.bar if");
    checkWrongSyntax("DROP PROPERTY Foo.bar if force");
  }
}
