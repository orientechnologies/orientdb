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
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static java.sql.ResultSet.*;
import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcConnectionTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertThat(stmt).isNotNull();
    stmt.close();
  }

  @Test
  public void checkSomePrecondition() throws Exception {

    assertThat(conn.isClosed()).isFalse();
    conn.isReadOnly();

    conn.isValid(0);
    conn.setAutoCommit(true);
    assertThat(conn.getAutoCommit()).isTrue();
    // conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    // assertEquals(Connection.TRANSACTION_NONE,
    // conn.getTransactionIsolation());
  }

  @Test
  public void shouldCreateDifferentTypeOfStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertThat(stmt).isNotNull();

    stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    assertThat(stmt).isNotNull();

    stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    assertThat(stmt).isNotNull();

  }

  @Test
  public void shouldConnectUsingPool() throws Exception {
    String dbUrl = "jdbc:orient:memory:OrientJdbcConnectionTest";
    Properties p = new Properties();
    p.setProperty("db.usePool", "TRUE");

    Connection connection = DriverManager.getConnection(dbUrl, p);

    assertThat(connection).isNotNull();
    connection.close();
  }

}
