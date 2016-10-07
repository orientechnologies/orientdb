package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class OrientDataSourceTest extends OrientJdbcBaseTest {

  @Test
  public void shouldConnect() throws SQLException {

    OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:OrientDataSourceTest");
    ds.setUsername("admin");
    ds.setPassword("admin");

    Connection conn = ds.getConnection();

    assertThat(conn).isNotNull();
    conn.close();
    assertThat(conn.isClosed()).isTrue();

    conn = ds.getConnection();

    assertThat(conn).isNotNull();
    conn.close();
    assertThat(conn.isClosed()).isTrue();

  }

  @Test
  public void shouldConnectWithPoolSizeOne() throws SQLException {

    Properties info = new Properties();
    info.setProperty("db.usePool", "TRUE");
    info.setProperty("db.pool.min", "1");
    info.setProperty("db.pool.max", "10");

    final OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:OrientDataSourceTest");
    ds.setUsername("admin");
    ds.setPassword("admin");
    ds.setInfo(info);

    //pool size is 1: database should be the same on different connection
    //NOTE: not safe in production!
    OrientJdbcConnection conn = (OrientJdbcConnection) ds.getConnection();
    assertThat(conn).isNotNull();

    OrientJdbcConnection conn2 = (OrientJdbcConnection) ds.getConnection();
    assertThat(conn2).isNotNull();
    conn.getDatabase();

    assertThat(conn.getDatabase()).isSameAs(conn2.getDatabase());

    conn.close();
    assertThat(conn.isClosed()).isTrue();

    conn2.close();
    assertThat(conn2.isClosed()).isTrue();

  }

  @Test
  public void shouldQueryWithPool() {
    final Properties info = new Properties();
    info.setProperty("db.usePool", "TRUE");
    info.setProperty("db.pool.min", "1");
    info.setProperty("db.pool.max", "10");

    final OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:OrientDataSourceTest");
    ds.setUsername("admin");
    ds.setPassword("admin");
    ds.setInfo(info);

    //do 10 queries and asserts on other thread
    Runnable dbClient = () -> {

      //do 10 queries
      IntStream.range(0, 9).forEach(i -> {
        Connection conn1 = null;
        try {
          conn1 = ds.getConnection();

          Statement statement = conn1.createStatement();
          ResultSet rs = statement.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

          assertThat(rs.first()).isTrue();
          assertThat(rs.getString("stringKey")).isEqualTo("1");

          rs.close();

          statement.close();
          conn1.close();
          assertThat(conn1.isClosed()).isTrue();
        } catch (SQLException e) {
          fail();
        }
      });
    };
    //spawn 20 threads
    List<CompletableFuture<Void>> futures = IntStream.range(0, 19).boxed()
        .map(i -> CompletableFuture.runAsync(dbClient))
        .collect(Collectors.toList());

    futures.forEach(cf -> cf.join());
  }

}
