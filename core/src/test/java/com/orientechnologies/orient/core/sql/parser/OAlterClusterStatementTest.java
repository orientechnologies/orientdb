package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterClusterStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER CLUSTER Foo name bar");
    checkRightSyntax("alter cluster Foo name bar");
    checkRightSyntax("alter cluster Foo* name bar");
  }
}
