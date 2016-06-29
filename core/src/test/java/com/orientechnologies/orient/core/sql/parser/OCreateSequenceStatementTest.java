package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateSequenceStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE SEQUENCE Foo TYPE CACHED");
    checkRightSyntax("CREATE SEQUENCE Foo TYPE ORDERED");
    checkRightSyntax("create sequence Foo type cached");
    checkRightSyntax("create sequence Foo type ordered");

    checkRightSyntax("create sequence Foo type ordered start 100 ");
    checkRightSyntax("create sequence Foo type ordered increment 4");
    checkRightSyntax("create sequence Foo type ordered cache 5");
    checkRightSyntax("create sequence Foo type ordered start 100 increment 4 cache 5");
    checkRightSyntax("create sequence Foo type ordered START 100 INCREMENT 4 CACHE 5");
    checkRightSyntax("create sequence Foo type ordered START :a INCREMENT :b CACHE :c");

    checkWrongSyntax("CREATE SEQUENCE Foo");
    checkWrongSyntax("CREATE SEQUENCE Foo TYPE foo");
  }

}
