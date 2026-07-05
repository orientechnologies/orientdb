package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OCreateDatabaseStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("CREATE DATABASE foo plocal");
    checkRightSyntaxServer("CREATE DATABASE ? plocal");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal {\"config\":{\"security.createDefaultUsers\": true}}");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin, reader identified"
            + " by ? role [reader, writer])");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " {\"config\":{\"security.createDefaultUsers\": true}}");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " nodes (node1 role main, node2 role main, node3 role replica)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " nodes (node1 role ?, node2 role ?)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " nodes (? role ?, ? role ?)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " nodes (? role main, ? role replica)");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)" + " nodes (?,?)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " nodes (node1,node2)");

    checkWrongSyntax("CREATE DATABASE foo");
    checkWrongSyntax("CREATE DATABASE");
  }
}
