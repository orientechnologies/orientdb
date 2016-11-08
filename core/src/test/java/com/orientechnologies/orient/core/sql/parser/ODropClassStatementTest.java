package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
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

  public void testIfExists(){
    checkRightSyntax("DROP CLASS Foo if exists");
    checkRightSyntax("DROP CLASS Foo IF EXISTS");
    checkRightSyntax("DROP CLASS if if exists");

    checkWrongSyntax("drop class Foo if");
    checkWrongSyntax("drop class Foo if exists lkj");
    checkWrongSyntax("drop class Foo if lkj");
  }

}
