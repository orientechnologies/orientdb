package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateSecurityPolicyStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE SECURITY POLICY foo");

    checkRightSyntax("CREATE SECURITY POLICY foo SET CREATE = (true)");
    checkRightSyntax("CREATE SECURITY POLICY foo SET read = (name = 'foo')");

    checkRightSyntax(
        "CREATE SECURITY POLICY foo SET CREATE = (name = 'foo'), READ = (name = 'foo')"
            + ", BEFORE UPDATE = (name = 'foo'), AFTER UPDATE = (name = 'foo'), DELETE = (name = 'foo'), EXECUTE = (name = 'foo')");

    checkWrongSyntax("CREATE SECURITY POLICY foo SET foo = (name = 'foo')");
  }
}
