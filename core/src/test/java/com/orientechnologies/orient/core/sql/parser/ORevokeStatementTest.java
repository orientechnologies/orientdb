package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class ORevokeStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("revoke UPDATE on database.class.Person from admin");
    checkRightSyntax("REVOKE CREATE on database.cluster.Person FROM admin");
    checkRightSyntax("revoke UPDATE on database.class.* from admin");
    checkRightSyntax("revoke DELETE on database.class.* from admin");
    checkRightSyntax("revoke NONE on database.class.* from admin");
    checkRightSyntax("revoke ALL on database.class.* from admin");
    checkRightSyntax("revoke EXECUTE on database.class.* from admin");
    checkRightSyntax("revoke execute on database.class.* from admin");

    checkRightSyntax("revoke POLICY on database.class.* from admin");

    checkWrongSyntax("revoke Foo on database.class.Person from admin");
  }
}
