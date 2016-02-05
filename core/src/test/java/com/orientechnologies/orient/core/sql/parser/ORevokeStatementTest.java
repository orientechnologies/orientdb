package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class ORevokeStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("revoke UPDATE on database.class.Person to admin");
    checkRightSyntax("REVOKE CREATE on database.cluster.Person to admin");
    checkRightSyntax("revoke UPDATE on database.class.* to admin");
    checkRightSyntax("revoke DELETE on database.class.* to admin");
    checkRightSyntax("revoke NONE on database.class.* to admin");
    checkRightSyntax("revoke ALL on database.class.* to admin");

    checkWrongSyntax("revoke Foo on database.class.Person to admin");
  }

}
