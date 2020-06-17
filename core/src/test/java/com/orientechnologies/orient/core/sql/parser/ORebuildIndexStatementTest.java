package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ORebuildIndexStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("REBUILD INDEX *");
    checkRightSyntax("REBUILD INDEX Foo");
    checkRightSyntax("rebuild index Foo");
    checkRightSyntax("REBUILD INDEX Foo.bar");
    checkRightSyntax("REBUILD INDEX Foo.bar.baz");
    checkWrongSyntax("REBUILD INDEX Foo.bar foo");
  }
}
