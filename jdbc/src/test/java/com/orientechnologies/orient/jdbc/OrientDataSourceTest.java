/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://orientdb.com
 */
package com.orientechnologies.orient.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class OrientDataSourceTest extends OrientJdbcDbPerClassTemplateTest {

  @Test
  public void shouldConnect() throws SQLException {

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
    info.setProperty("db.pool.max", "1");
    info.put("serverUser", "admin");
    info.put("serverPassword", "admin");

    final OrientDataSource ds = new OrientDataSource();
    ds.setDbUrl("jdbc:orient:memory:" + name.getMethodName());
    ds.setUsername("admin");
    ds.setPassword("admin");
    ds.setInfo(info);

    OrientJdbcConnection conn = (OrientJdbcConnection) ds.getConnection();
    assertThat(conn).isNotNull();

    conn.close();
    assertThat(conn.isClosed()).isTrue();

    OrientJdbcConnection conn2 = (OrientJdbcConnection) ds.getConnection();
    assertThat(conn2).isNotNull();
    conn2.close();
    assertThat(conn2.isClosed()).isTrue();

    assertThat(conn.getDatabase()).isSameAs(conn2.getDatabase());

    ds.close();
  }

  @Test
  public void shouldQueryWithPool() throws SQLException {

    ds.getConnection().close();
    // do 10 queries and asserts on other thread
    Runnable dbClient =
        () -> {

          // do 10 queries
          IntStream.range(0, 9)
              .forEach(
                  i -> {
                    Connection conn1 = null;
                    try {
                      conn1 = ds.getConnection();

                      Statement statement = conn1.createStatement();
                      ResultSet rs =
                          statement.executeQuery(
                              "SELECT stringKey, intKey, text, length, date FROM Item");

                      assertThat(rs.first()).isTrue();
                      assertThat(rs.getString("stringKey")).isEqualTo("1");

                      rs.close();

                      statement.close();
                    } catch (SQLException e) {
                      e.printStackTrace();
                      fail();
                    } finally {
                      if (conn1 != null) {
                        try {
                          conn1.close();
                          assertThat(conn1.isClosed()).isTrue();
                        } catch (SQLException e) {
                          e.printStackTrace();
                          fail();
                        }
                      }
                    }
                  });
        };
    // spawn 20 threads
    List<CompletableFuture<Void>> futures =
        IntStream.range(0, 19)
            .boxed()
            .map(i -> CompletableFuture.runAsync(dbClient))
            .collect(Collectors.toList());

    futures.forEach(CompletableFuture::join);
  }

  @Test
  public void shouldConnectWithOrientDBInstance() throws SQLException {
    final String serverUser = "admin";
    final String serverPassword = "admin";
    final String dbName = "test";

    OrientDB orientDB =
        new OrientDB("embedded:.", serverUser, serverPassword, OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users(admin identified by 'admin' role admin)", dbName);

    OrientDataSource ods = new OrientDataSource(orientDB, dbName);
    Connection connection = ods.getConnection(serverUser, serverPassword);
    Statement statement = connection.createStatement();
    statement.executeQuery("SELECT FROM V");
  }
}
