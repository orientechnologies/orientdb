package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterPropertyStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER PROPERTY Foo.foo NAME Bar");
    checkRightSyntax("alter property Foo.foo NAME Bar");
    checkRightSyntax("ALTER PROPERTY Foo.foo REGEXP \"[M|F]\"");
    checkRightSyntax("ALTER PROPERTY Foo.foo CUSTOM foo = 'bar'");
    checkRightSyntax("ALTER PROPERTY Foo.foo CUSTOM foo = bar()");
    checkRightSyntax("alter property Foo.foo custom foo = bar()");
  }
}
