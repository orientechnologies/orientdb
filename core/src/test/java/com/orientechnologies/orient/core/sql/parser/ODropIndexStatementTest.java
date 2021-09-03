package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropIndexStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP INDEX *");
    checkRightSyntax("DROP INDEX Foo");
    checkRightSyntax("drop index Foo");
    checkRightSyntax("DROP INDEX Foo.bar");
    checkRightSyntax("DROP INDEX Foo.bar.baz");
    checkRightSyntax("DROP INDEX Foo.bar.baz if exists");
    checkRightSyntax("DROP INDEX Foo.bar.baz IF EXISTS");
    checkWrongSyntax("DROP INDEX Foo.bar foo");
  }
}
