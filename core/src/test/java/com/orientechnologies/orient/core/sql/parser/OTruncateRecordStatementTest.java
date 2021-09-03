package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OTruncateRecordStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("TRUNCATE RECORD #12:0");
    checkRightSyntax("truncate record #12:0");
    checkRightSyntax("TRUNCATE RECORD [#12:0]");
    checkRightSyntax("TRUNCATE RECORD [ #12:0 ]");
    checkRightSyntax("TRUNCATE RECORD [ #12:0, #12:1, #12:2 ]");

    checkWrongSyntax("TRUNCATE RECORD");
    checkWrongSyntax("TRUNCATE RECORD #12:0, #12:1, #12:2 ");
  }
}
