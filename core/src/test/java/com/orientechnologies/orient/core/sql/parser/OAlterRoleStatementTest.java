package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterRoleStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER ROLE Foo SET POLICY bar ON database.class.Person");
    checkRightSyntax("ALTER ROLE Foo REMOVE POLICY ON database.class.Person");

    checkRightSyntax(
        "ALTER ROLE Foo  SET POLICY bar ON database.class.Person  "
            + "SET POLICY bar ON database.class.Xx REMOVE POLICY ON database.class.Person");

    checkRightSyntax(
        "alter role Foo  set policy bar on database.class.Person  "
            + "set policy bar on database.class.Xx remove policy on database.class.Person");

    checkWrongSyntax("alter role foo");
  }
}
