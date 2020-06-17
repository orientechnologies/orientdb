package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropSequenceStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {

    checkRightSyntax("DROP SEQUENCE Foo");
    checkRightSyntax("drop sequence Foo");
    checkRightSyntax("drop sequence `Foo.bar`");
    checkRightSyntax("drop sequence Foo IF EXISTS");

    checkWrongSyntax("drop SEQUENCE Foo IF NOT EXISTS");
    checkWrongSyntax("drop SEQUENCE Foo TYPE cached");
  }
}
