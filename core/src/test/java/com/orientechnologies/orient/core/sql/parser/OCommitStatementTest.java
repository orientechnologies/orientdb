package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OCommitStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("COMMIT");
    checkRightSyntax("commit");

    checkRightSyntax("COMMIT RETRY 10");
    checkRightSyntax("commit retry 10");

    checkWrongSyntax("COMMIT RETRY 10.1");
    checkWrongSyntax("COMMIT RETRY 10,1");
    checkWrongSyntax("COMMIT RETRY foo");
    checkWrongSyntax("COMMIT RETRY");
    checkWrongSyntax("COMMIT 10.1");  }
}
