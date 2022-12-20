package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateViewStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE VIEW Foo FROM (select from v where name ='foo')");
    checkRightSyntax("create view Foo from (select from v where name ='foo')");
    checkRightSyntax("create view Foo from (MATCH {class:V} RETURN $elements)");

    checkRightSyntax(
        "CREATE VIEW Foo FROM (select from v where name ='foo') METADATA {updatable:true, originRidField:'origin'}");

    checkWrongSyntax("create view Foo");
    checkRightSyntax(
        "CREATE VIEW Foo FROM (select from v where name ='foo') METADATA {indexes:[{type:'UNIQUE', properties:{name:'STRING'}}]}");
    checkRightSyntax(
        "CREATE VIEW Foo FROM (select from v where name ='foo') METADATA {indexes:[{\"type\":\"unique\", \"properties\":{\"ids\":{\"type\":\"EMBEDDEDEDSET\",\"linkedType\":\"STRING\"}}}]}");
  }

  @Test
  public void testIfNotExists() {
    checkRightSyntax("CREATE VIEW Foo if not exists from (SELECT FROM V)");
  }
}
