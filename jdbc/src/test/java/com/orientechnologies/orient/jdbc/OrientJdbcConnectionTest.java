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

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.junit.Test;

public class OrientJdbcConnectionTest extends OrientJdbcDbPerClassTemplateTest {

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
    p.setProperty("serverUser", "root");
    p.setProperty("serverPassword", "root");

    Connection connection = DriverManager.getConnection(dbUrl, p);

    assertThat(connection).isNotNull();
    connection.close();
  }
}
