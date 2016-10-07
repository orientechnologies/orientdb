package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OHaSyncClusterStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("HA SYNC CLUSTER foo");

    checkRightSyntax("HA SYNC CLUSTER foo -merge");
    checkRightSyntax("HA SYNC CLUSTER foo -full_replace");
    checkRightSyntax("ha sync cluster foo");

    checkWrongSyntax("HA SYNC CLUSTER");
    checkWrongSyntax("HA SYNC CLUSTER foo bar");
    checkWrongSyntax("HA SYNC CLUSTER foo -full_replace -merge");
    checkWrongSyntax("HA SYNC CLUSTER -full_replace ");
  }

}
