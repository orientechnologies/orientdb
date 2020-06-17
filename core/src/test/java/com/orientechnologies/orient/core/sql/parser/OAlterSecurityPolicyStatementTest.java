package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterSecurityPolicyStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER SECURITY POLICY foo SET CREATE = (foo = 'bar')");
    checkRightSyntax("ALTER SECURITY POLICY foo REMOVE CREATE");
    checkRightSyntax("ALTER SECURITY POLICY foo SET CREATE = (foo = 'bar') REMOVE DELETE");

    checkRightSyntax(
        "ALTER SECURITY POLICY foo SET CREATE = (name = 'foo'), READ = (name = 'foo')"
            + ", BEFORE UPDATE = (name = 'foo'), AFTER UPDATE = (name = 'foo'), DELETE = (name = 'foo'), EXECUTE = (name = 'foo')");

    checkRightSyntax(
        "ALTER SECURITY POLICY foo REMOVE CREATE, READ, BEFORE UPDATE, AFTER UPDATE, DELETE, EXECUTE");

    checkRightSyntax(
        "ALTER SECURITY POLICY foo SET CREATE = (name = 'foo'), READ = (name = 'foo')"
            + ", BEFORE UPDATE = (name = 'foo'), AFTER UPDATE = (name = 'foo'), DELETE = (name = 'foo'), EXECUTE = (name = 'foo') "
            + "REMOVE CREATE, READ, BEFORE UPDATE, AFTER UPDATE, DELETE, EXECUTE");

    checkWrongSyntax("ALTER SECURITY POLICY foo");
  }
}
