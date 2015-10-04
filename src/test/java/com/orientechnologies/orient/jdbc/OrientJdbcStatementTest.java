package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class OrientJdbcStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertThat(stmt, is(notNullValue()));
    stmt.close();
    assertThat(stmt.isClosed(), is(true));

  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    Statement stmt = conn.createStatement();
    assertThat(stmt.execute(""), is(false));
    assertThat(stmt.getResultSet(), is(nullValue()));
    assertThat(stmt.getMoreResults(), is(false));
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {

    Statement st = conn.createStatement();
    assertThat(st.execute("select 1"), is(true));
    assertThat(st.getResultSet(), is(notNullValue()));
    ResultSet resultSet = st.getResultSet();
    resultSet.first();
    assertThat(resultSet.getInt("1"), is(1));
    assertThat(st.getMoreResults(), is(false));

  }

}
