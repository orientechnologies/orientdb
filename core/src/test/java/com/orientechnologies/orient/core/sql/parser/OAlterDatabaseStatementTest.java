package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterDatabaseStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER DATABASE CLUSTERSELECTION 'default'");
    checkRightSyntax("alter database CLUSTERSELECTION 'default'");

    checkRightSyntax("alter database custom strictSql=false");

    checkWrongSyntax("alter database ");
    checkWrongSyntax("alter database bar baz zz");
  }
}
