package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OGrantStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("grant UPDATE on database.class.Person to admin");
    checkRightSyntax("GRANT CREATE on database.cluster.Person to admin");
    checkRightSyntax("grant UPDATE on database.class.* to admin");
    checkRightSyntax("grant DELETE on database.class.* to admin");
    checkRightSyntax("grant NONE on database.class.* to admin");
    checkRightSyntax("grant ALL on database.class.* to admin");
    checkRightSyntax("grant EXECUTE on database.class.* to admin");
    checkRightSyntax("grant execute on database.class.* to admin");

    checkRightSyntax("grant policy foo on database.class.* to admin");

    checkWrongSyntax("grant Foo on database.class.Person to admin");
    checkWrongSyntax("grant policy on database.class.* to admin");
  }
}
