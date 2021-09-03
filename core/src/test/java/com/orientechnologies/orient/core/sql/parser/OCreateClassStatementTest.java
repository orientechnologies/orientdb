package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

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

  @Test
  public void testIfNotExists() {
    checkRightSyntax("CREATE CLASS Foo if not exists");
    checkRightSyntax("CREATE CLASS Foo IF NOT EXISTS");
    checkRightSyntax("CREATE CLASS Foo if not exists extends V");

    checkWrongSyntax("CREATE CLASS Foo if");
    checkWrongSyntax("CREATE CLASS Foo if not");
  }
}
