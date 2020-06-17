package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OIfStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("if(1=1){return foo;}");
    checkRightSyntax("IF(1=1){return foo;}");

    checkRightSyntax("if(1=1){\n" + "return foo;" + "\n}");

    checkRightSyntax("if(1=1){\n" + "/* foo bar baz */" + "return foo;" + "\n}");
    checkRightSyntax(
        "if(1=1){\n"
            + "/* foo bar baz */"
            + "update foo set name = 'bar';"
            + "return foo;"
            + "\n}");

    checkRightSyntax(
        "if\n(1=1){\n"
            + "/* foo bar baz */"
            + "update foo set name = 'bar';"
            + "return foo;"
            + "\n}");
  }
}
