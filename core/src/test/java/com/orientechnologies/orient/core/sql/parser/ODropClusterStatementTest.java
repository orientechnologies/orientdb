package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class ODropClusterStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP CLUSTER Foo");
    checkRightSyntax("drop cluster Foo");
    checkRightSyntax("DROP CLUSTER 14");

    checkWrongSyntax("DROP CLUSTER foo 14");
    checkWrongSyntax("DROP CLUSTER foo bar");
    checkWrongSyntax("DROP CLUSTER 14.1");
    checkWrongSyntax("DROP CLUSTER 14 1");
  }

}
