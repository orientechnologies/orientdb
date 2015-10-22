package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
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

  @Test
  public void shouldConnectPooled() throws SQLException {

    OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:test");
    ds.setUsername("admin");
    ds.setPassword("admin");

    Properties info = new Properties();
    info.setProperty("db.usePool", "TRUE");
    info.setProperty("db.pool.min", "1");
    info.setProperty("db.pool.max", "1");

    //pool size is 1: database should be the same on different connection
    //NOTE: not safe in production!
    OrientJdbcConnection conn = (OrientJdbcConnection) ds.getConnection(info);
    assertThat(conn, is(notNullValue()));

    OrientJdbcConnection conn2 = (OrientJdbcConnection) ds.getConnection(info);
    assertThat(conn2, is(notNullValue()));
    conn.getDatabase();

    assertThat(conn.getDatabase(), is(sameInstance(conn2.getDatabase())));

    conn.close();
    assertThat(conn.isClosed(), is(true));

    conn2.close();
    assertThat(conn2.isClosed(), is(true));


  }

}
