package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OCreateClassStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE CLASS Foo");
    checkRightSyntax("create class Foo");
    checkRightSyntax("create class Foo extends bar, baz cluster 12, 13, 14 clusters 5 abstract");
    checkRightSyntax("CREATE CLASS Foo EXTENDS bar, baz CLUSTER 12, 13, 14 CLUSTERS 5 ABSTRACT");
    checkRightSyntax("CREATE CLASS Foo EXTENDS bar, baz CLUSTER 12,13, 14 CLUSTERS 5 ABSTRACT");

    checkWrongSyntax("CREATE CLASS Foo EXTENDS ");
    checkWrongSyntax("CREATE CLASS Foo CLUSTER ");
    checkWrongSyntax("CREATE CLASS Foo CLUSTERS ");
    checkWrongSyntax("CREATE CLASS Foo CLUSTERS 1,2 ");
  }

}
