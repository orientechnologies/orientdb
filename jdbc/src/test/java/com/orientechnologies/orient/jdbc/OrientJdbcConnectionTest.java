package com.orientechnologies.orient.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.hamcrest.Matchers;
import org.junit.Test;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

public class OrientJdbcConnectionTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertNotNull(stmt);
    stmt.close();
  }

  @Test
  public void checkSomePrecondition() throws Exception {

    assertFalse(conn.isClosed());
    conn.isReadOnly();

    conn.isValid(0);
    conn.setAutoCommit(true);
    assertTrue(conn.getAutoCommit());
    // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    // assertEquals(Connection.TRANSACTION_NONE,
    // conn.getTransactionIsolation());
  }

  @Test
  public void shouldCreateDifferentTypeOfStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertNotNull(stmt);

    stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    assertNotNull(stmt);

    stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    assertNotNull(stmt);

  }

  @Test
  public void shouldConnectUsingPool() throws Exception {
    String dbUrl = "jdbc:orient:memory:test";
    Properties p = new Properties();
    p.setProperty("db.usePool", "TRUE");

    Connection connection = DriverManager.getConnection(dbUrl, p);
    assertNotNull(connection);

    assertThat(connection,is(notNullValue()));
    connection.close();
  }

}
