package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateIndexStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE INDEX Foo DICTIONARY");

    checkWrongSyntax("CREATE INDEX Foo");
    checkRightSyntax("CREATE INDEX Foo.bar DICTIONARY");

    checkRightSyntax("CREATE INDEX Foo.bar on Foo (bar) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, @rid) UNIQUE");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE");

    checkRightSyntax("create index Foo.bar on Foo (bar collate CI) UNIQUE");

    checkRightSyntax(
        "create index collateTestViaSQL.index on collateTestViaSQL (cip COLLATE CI) NOTUNIQUE");

    checkRightSyntax("CREATE INDEX Foo.bar on Foo (bar) UNIQUE METADATA {'foo':'bar'}");
    checkRightSyntax("CREATE INDEX Foo.bar_baz on Foo (bar, baz) UNIQUE METADATA {'foo':'bar'}");
    checkRightSyntax(
        "CREATE INDEX Foo.bar_baz on Foo (bar by key, baz by value) UNIQUE METADATA {'foo':'bar'}");

    checkRightSyntax("CREATE INDEX test unique string,string");
    checkRightSyntax("create index OUser.name UNIQUE ENGINE SBTREE STRING");
    checkRightSyntax("create index OUser.name UNIQUE engine SBTREE STRING");

    checkRightSyntax("CREATE INDEX Foo.bar IF NOT EXISTS on Foo (bar) UNIQUE");

    checkWrongSyntax("CREATE INDEX Foo.bar IF EXISTS on Foo (bar) UNIQUE");
  }
}
