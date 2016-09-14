package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;
public class ODropClassStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP CLASS Foo");
    checkRightSyntax("drop class Foo");
    checkRightSyntax("drop class Foo UNSAFE");
    checkRightSyntax("drop class Foo unsafe");
    checkRightSyntax("DROP CLASS `Foo bar`");

    checkWrongSyntax("drop class Foo UNSAFE foo");
    checkWrongSyntax("drop class Foo bar");
  }

}
