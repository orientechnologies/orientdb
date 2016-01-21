package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcDriverTest extends OrientJdbcBaseTest {

  @Test
  public void shouldAcceptsWellFormattedURLOnly() throws ClassNotFoundException, SQLException {

    Driver drv = new OrientJdbcDriver();

    assertThat(drv.acceptsURL("jdbc:orient:local:./working/db/test")).isTrue();
    assertThat(drv.acceptsURL("local:./working/db/test")).isFalse();
  }

  @Test
  public void shouldConnect() throws SQLException {

    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");

    OrientJdbcConnection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:memory:test", info);

    assertThat(conn).isNotNull();
    conn.close();
    assertThat(conn.isClosed()).isTrue();
  }
}
