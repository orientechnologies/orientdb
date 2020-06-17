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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class OrientJdbcStatementTest extends OrientJdbcDbPerClassTemplateTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertThat(stmt).isNotNull();
    stmt.close();
    assertThat(stmt.isClosed()).isTrue();
  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("")).isFalse();
    assertThat(stmt.getResultSet()).isNull();
    ;
    assertThat(stmt.getMoreResults()).isFalse();
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {

    Statement st = conn.createStatement();
    assertThat(st.execute("select 1")).isTrue();
    assertThat(st.getResultSet()).isNotNull();
    ResultSet resultSet = st.getResultSet();
    resultSet.first();
    assertThat(resultSet.getInt("1")).isEqualTo(1);
    assertThat(st.getMoreResults()).isFalse();
  }

  @Test(expected = SQLException.class)
  public void shouldThrowSqlExceptionOnError() throws SQLException {

    String query = String.format("select sequence('%s').next()", "theSequence");
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);
  }
}
