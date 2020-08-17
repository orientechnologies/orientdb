package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateDatabaseStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("CREATE DATABASE foo plocal");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal {\"config\":{\"security.createDefaultUsers\": true}}");

    checkWrongSyntax("CREATE DATABASE foo");
    checkWrongSyntax("CREATE DATABASE");
  }
}
