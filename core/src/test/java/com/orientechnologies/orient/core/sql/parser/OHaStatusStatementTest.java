package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OHaStatusStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("HA STATUS");
    checkRightSyntax("HA STATUS -servers");
    checkRightSyntax("HA STATUS -db");
    checkRightSyntax("HA STATUS -latency");
    checkRightSyntax("HA STATUS -messages");
    checkRightSyntax("HA STATUS -all");
    checkRightSyntax("HA STATUS -output=text");

    checkRightSyntax("HA STATUS -servers -db -latency -messages -output=text");

    checkWrongSyntax("HA STATUS servers");
    checkWrongSyntax("HA STATUS db");
  }

}
