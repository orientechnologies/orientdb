package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterSystemRoleStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("ALTER SYSTEM ROLE Foo SET POLICY bar ON database.class.Person");
    checkRightSyntaxServer("ALTER SYSTEM ROLE Foo REMOVE POLICY ON database.class.Person");

    checkRightSyntaxServer(
        "ALTER SYSTEM ROLE Foo  SET POLICY bar ON database.class.Person  "
            + "SET POLICY bar ON database.class.Xx REMOVE POLICY ON database.class.Person");

    checkRightSyntaxServer(
        "alter system role Foo  set policy bar on database.class.Person  "
            + "set policy bar on database.class.Xx remove policy on database.class.Person");

    checkWrongSyntaxServer("alter system role foo");
  }
}
