package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OCreatePropertyStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE PROPERTY Foo.bar STRING");
    checkRightSyntax("create property Foo.bar STRING");

    checkRightSyntax("CREATE PROPERTY Foo.bar LINK Bar");
    checkRightSyntax("CREATE PROPERTY Foo.bar LINK Bar unsafe");

    checkRightSyntax("CREATE PROPERTY `Foo bar`.`bar baz` LINK Bar unsafe");
  }

}
