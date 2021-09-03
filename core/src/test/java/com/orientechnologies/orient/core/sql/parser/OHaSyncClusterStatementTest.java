package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OHaSyncClusterStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA SYNC CLUSTER foo");
    checkRightSyntax("ha sync cluster foo");
    checkRightSyntax("HA SYNC CLUSTER foo -full_replace");
    checkRightSyntax("HA SYNC CLUSTER foo -merge");

    checkWrongSyntax("HA SYNC CLUSTER foo -foo");
    checkWrongSyntax("HA SYNC CLUSTER");
  }
}
