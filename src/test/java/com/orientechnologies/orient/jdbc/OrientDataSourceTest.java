package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class OrientDataSourceTest extends OrientJdbcBaseTest {

  @Test
  public void shouldConnect() throws SQLException {

    OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:test");
    ds.setUsername("admin");
    ds.setPassword("admin");

    Connection conn = ds.getConnection();

    assertThat(conn, is(notNullValue()));
    conn.close();
    assertThat(conn.isClosed(), is(true));

  }

}
