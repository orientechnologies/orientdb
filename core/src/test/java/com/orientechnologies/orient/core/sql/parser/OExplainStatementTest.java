package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OExplainStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax(
        "EXPLAIN SELECT FROM Foo WHERE name = 'bar' and surname in (select surname from Baz)");
    checkRightSyntax(
        "explain SELECT FROM Foo WHERE name = 'bar' and surname in (select surname from Baz)");
  }
}
