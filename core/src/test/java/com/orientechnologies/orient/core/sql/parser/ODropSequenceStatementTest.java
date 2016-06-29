package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropSequenceStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {

    checkRightSyntax("DROP SEQUENCE Foo");
    checkRightSyntax("drop sequence Foo");
    checkRightSyntax("drop sequence `Foo.bar`");

    checkWrongSyntax("drop SEQUENCE Foo TYPE cached");
  }

}
