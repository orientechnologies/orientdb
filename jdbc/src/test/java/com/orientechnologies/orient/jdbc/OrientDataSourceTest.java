/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://orientdb.com
 */
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

    futures.forEach(CompletableFuture::join);
  }

}
