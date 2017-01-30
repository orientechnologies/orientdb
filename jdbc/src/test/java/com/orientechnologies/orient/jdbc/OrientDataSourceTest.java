package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
  public void shouldConnectWithPoolSizeTen() throws Exception {

    final Properties info = new Properties();
    info.setProperty("db.usePool", "TRUE");
    info.setProperty("db.pool.min", "1");
    info.setProperty("db.pool.max", "10");

    final OrientDataSource ds = new OrientDataSource();
    ds.setUrl("jdbc:orient:memory:OrientDataSourceTest");
    ds.setUsername("admin");
    ds.setPassword("admin");
    ds.setInfo(info);

    final AtomicBoolean queryTheDb = new AtomicBoolean(true);
    Callable<Boolean> dbClient = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {

        while (queryTheDb.get()) {

          try {

            Connection conn = ds.getConnection();

            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

            assertThat(rs.first()).isTrue();
            assertThat(rs.getString("stringKey")).isEqualTo("1");

            rs.close();

            statement.close();
            conn.close();
            assertThat(conn.isClosed()).isTrue();

          } catch (Exception e) {
            e.printStackTrace();
            fail("fail:::", e);
          }
        }

        return Boolean.TRUE;
      }
    };

    ExecutorService pool = Executors.newCachedThreadPool();

    //activate 4 clients ≈
    pool.submit(dbClient);
    pool.submit(dbClient);
    pool.submit(dbClient);
    pool.submit(dbClient);

    //and let them work
    TimeUnit.SECONDS.sleep(5);

    //stop clients
    queryTheDb.set(false);

    pool.shutdown();

  }

}
