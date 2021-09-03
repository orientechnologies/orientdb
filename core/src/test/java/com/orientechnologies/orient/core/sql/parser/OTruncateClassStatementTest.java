package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OTruncateClassStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE CLASS Foo");
    checkRightSyntax("truncate class Foo");
    checkRightSyntax("TRUNCATE CLASS Foo polymorphic");
    checkRightSyntax("truncate class Foo POLYMORPHIC");
    checkRightSyntax("TRUNCATE CLASS Foo unsafe");
    checkRightSyntax("truncate class Foo UNSAFE");
    checkRightSyntax("TRUNCATE CLASS Foo polymorphic unsafe");
    checkRightSyntax("truncate class Foo POLYMORPHIC UNSAFE");
    checkWrongSyntax("TRUNCATE CLASS Foo polymorphic unsafe FOO");
    checkRightSyntax("truncate class `Foo bar` ");
    checkWrongSyntax("truncate class Foo bar ");
    checkWrongSyntax("truncate clazz Foo ");
  }
}
