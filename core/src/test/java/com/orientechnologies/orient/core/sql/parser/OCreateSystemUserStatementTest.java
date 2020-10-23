package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateSystemUserStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("CREATE SYSTEM USER test IDENTIFIED BY foo");
    checkRightSyntaxServer("CREATE SYSTEM USER test IDENTIFIED BY 'foo'");
    checkRightSyntaxServer("CREATE SYSTEM USER test IDENTIFIED BY 'foo' ROLE admin");
    checkRightSyntaxServer("CREATE SYSTEM USER test IDENTIFIED BY 'foo' ROLE [admin, reader]");
    checkRightSyntaxServer("create system user test identified by 'foo' role [admin, reader]");
    checkWrongSyntaxServer("CREATE SYSTEM USER test IDENTIFIED BY 'foo' role admin, reader");
    checkWrongSyntaxServer("CREATE SYSTEM USER test");
  }
}
