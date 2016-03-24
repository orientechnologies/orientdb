package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OCreateClusterStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE CLUSTER Foo");
    checkRightSyntax("CREATE CLUSTER Foo ID 14");
    checkRightSyntax("create cluster Foo");
    checkRightSyntax("create cluster Foo id 14");
    checkRightSyntax("CREATE BLOB CLUSTER Foo");
    checkRightSyntax("create blob cluster Foo id 14");


    checkWrongSyntax("CREATE Cluster");
    checkWrongSyntax("CREATE Cluster foo bar");
    checkWrongSyntax("CREATE Cluster foo.bar");
    checkWrongSyntax("CREATE Cluster foo id bar");
  }

}
