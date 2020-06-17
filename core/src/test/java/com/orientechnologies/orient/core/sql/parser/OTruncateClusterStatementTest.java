package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OTruncateClusterStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE CLUSTER Foo");
    checkRightSyntax("truncate cluster Foo");

    checkRightSyntax("TRUNCATE CLUSTER 12");
    checkRightSyntax("truncate cluster 12");

    checkRightSyntax("TRUNCATE CLUSTER Foo unsafe");

    checkRightSyntax("TRUNCATE CLUSTER `Foo bar`");

    checkWrongSyntax("TRUNCATE CsUSTER Foo");
    checkWrongSyntax("truncate cluster Foo bar");
  }
}
