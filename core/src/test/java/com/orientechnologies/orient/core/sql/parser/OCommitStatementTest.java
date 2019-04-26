package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;
public class OCommitStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("COMMIT");
    checkRightSyntax("commit");

    checkRightSyntax("COMMIT RETRY 10");
    checkRightSyntax("commit retry 10");

    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';}");
    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo'; INSERT INTO A SET name = 'Bar';}");

    checkWrongSyntax("COMMIT RETRY 10.1");
    checkWrongSyntax("COMMIT RETRY 10,1");
    checkWrongSyntax("COMMIT RETRY foo");
    checkWrongSyntax("COMMIT RETRY");
    checkWrongSyntax("COMMIT 10.1");
    checkWrongSyntax("commit retry 10 ELSE {}");
    checkWrongSyntax("commit retry 10 ELSE");
    checkWrongSyntax("commit ELSE {INSERT INTO A SET name = 'Foo';}");
  }


}
