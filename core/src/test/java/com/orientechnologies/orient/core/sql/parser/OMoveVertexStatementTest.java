package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

@Test
public class OMoveVertexStatementTest extends OParserTestAbstract {

  public void testPlain() {
    checkRightSyntax("Move Vertex V to class:bar");
    checkRightSyntax("Move Vertex V to cluster:bar");
    checkRightSyntax("Move Vertex V to cluster:12");
    checkRightSyntax("Move Vertex (Select from v) to CLASS:bar");
    checkRightSyntax("Move Vertex (Select from v) to CLASS:bar set name = 'a'");

  }

}
