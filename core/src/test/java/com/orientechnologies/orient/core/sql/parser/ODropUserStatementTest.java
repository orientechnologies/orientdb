package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ODropUserStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP USER test");

    checkWrongSyntax("DROP USER test IDENTIFIED BY 'foo'");
    checkWrongSyntax("DROP USER");
  }
}
