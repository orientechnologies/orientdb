package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcStatementTest extends OrientJdbcBaseTest {

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
  public void shouldTrhowSqlExceptionOnError() throws SQLException {

    String query = String.format("select sequence('%s').next()", "theSequence");
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);


  }
}
