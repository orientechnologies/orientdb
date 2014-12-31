package com.orientechnologies.orient.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrientJdbcDriverTest extends OrientJdbcBaseTest {

  @Test
  public void shouldAcceptsWellFormattedURLOnly() throws ClassNotFoundException, SQLException {

    Driver drv = new OrientJdbcDriver();

    assertTrue(drv.acceptsURL("jdbc:orient:local:./working/db/test"));

    assertFalse(drv.acceptsURL("local:./working/db/test"));
  }

  @Test
  public void shouldConnect() throws SQLException {

    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");

    OrientJdbcConnection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:memory:test", info);

    assertNotNull(conn);
    conn.close();
    assertTrue(conn.isClosed());
  }
}
