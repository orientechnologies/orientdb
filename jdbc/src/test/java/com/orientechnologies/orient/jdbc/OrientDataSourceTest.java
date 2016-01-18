package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class OrientDataSourceTest extends OrientJdbcBaseTest {

  @Test
  public void shouldConnect() throws SQLException {

    OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:test");
    ds.setUsername("admin");
    ds.setPassword("admin");

    Connection conn = ds.getConnection();

    assertThat(conn).isNotNull();
    conn.close();
    assertThat(conn.isClosed()).isTrue();

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
    assertThat(conn).isNotNull();

    OrientJdbcConnection conn2 = (OrientJdbcConnection) ds.getConnection(info);
    assertThat(conn2).isNotNull();
    conn.getDatabase();

    assertThat(conn.getDatabase()).isSameAs(conn2.getDatabase());

    conn.close();
    assertThat(conn.isClosed()).isTrue();

    conn2.close();
    assertThat(conn2.isClosed()).isTrue();

  }

}
