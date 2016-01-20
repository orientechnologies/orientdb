package com.orientechnologies.orient.jdbc;

import org.hamcrest.Matchers;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
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
    public void shouldConnectWithPoolSizeOne() throws SQLException {

        Properties info = new Properties();
        info.setProperty("db.usePool", "TRUE");
        info.setProperty("db.pool.min", "1");
        info.setProperty("db.pool.max", "1");

        final OrientDataSource ds = new OrientDataSource("jdbc:orient:memory:test", "admin", "admin", info);

        //pool size is 1: database should be the same on different connection
        //NOTE: not safe in production!
        OrientJdbcConnection conn = (OrientJdbcConnection) ds.getConnection();
        assertThat(conn, notNullValue());

        OrientJdbcConnection conn2 = (OrientJdbcConnection) ds.getConnection();
        assertThat(conn2, notNullValue());
        conn.getDatabase();

        assertThat(conn.getDatabase(), Matchers.sameInstance(conn2.getDatabase()));

        conn.close();
        assertThat(conn.isClosed(), Matchers.is(true));

        conn2.close();
        assertThat(conn2.isClosed(), Matchers.is(true));

    }

    @Test
    public void shouldConnectWithPoolSizeTen() throws Exception {

        final Properties info = new Properties();
        info.setProperty("db.usePool", "TRUE");
        info.setProperty("db.pool.min", "1");
        info.setProperty("db.pool.max", "10");

        final OrientDataSource ds = new OrientDataSource();
        ds.setUrl("jdbc:orient:memory:test");
        ds.setUsername("admin");
        ds.setPassword("admin");
        ds.setInfo(info);

        final AtomicBoolean queryTheDb = new AtomicBoolean(true);
        Callable<Boolean> dbClient = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {

                while (queryTheDb.get()) {

                    Connection conn = ds.getConnection();

                    Statement statement = conn.createStatement();
                    ResultSet rs = statement
                            .executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

                    assertThat(rs.first(), Matchers.is(true));
                    assertThat(rs.getString("stringKey"), Matchers.equalTo("1"));

                    rs.close();

                    statement.close();
                    conn.close();
                    assertThat(conn.isClosed(), Matchers.is(true));

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
        TimeUnit.SECONDS.sleep(2);

        //stop clients
        queryTheDb.set(false);

        pool.shutdown();

    }

}
