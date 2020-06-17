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
    checkRightSyntax(
        "commit retry 10 ELSE {INSERT INTO A SET name = 'Foo'; INSERT INTO A SET name = 'Bar';}");

    checkRightSyntax("commit retry 10 ELSE CONTINUE");
    checkRightSyntax("commit retry 10 ELSE FAIL");
    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND CONTINUE");
    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND FAIL");

    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} and continue");
    checkRightSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} and fail");

    checkWrongSyntax("COMMIT RETRY 10.1");
    checkWrongSyntax("COMMIT RETRY 10,1");
    checkWrongSyntax("COMMIT RETRY foo");
    checkWrongSyntax("COMMIT RETRY");
    checkWrongSyntax("COMMIT 10.1");
    checkWrongSyntax("commit retry 10 ELSE {}");
    checkWrongSyntax("commit retry 10 ELSE");
    checkWrongSyntax("commit ELSE {INSERT INTO A SET name = 'Foo';}");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} AND ");
    checkWrongSyntax("commit retry 10 ELSE AND CONTINUE");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} CONTINUE");
    checkWrongSyntax("commit retry 10 ELSE {INSERT INTO A SET name = 'Foo';} FAIL");
  }
}
