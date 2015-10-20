package com.orientechnologies.orient.jdbc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

public class OrientDataSourceTest extends OrientJdbcBaseTest {

  @Test
  public void shouldConnect() throws SQLException {

    OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:test");
    ds.setUsername("admin");
    ds.setPassword("admin");

    Connection conn = ds.getConnection();

    assertNotNull(conn);
    conn.close();
    assertTrue(conn.isClosed());

  }

}
