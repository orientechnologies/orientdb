package com.orientechnologies.orient.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OrientJdbcStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    Statement stmt = conn.createStatement();
    assertNotNull(stmt);
    stmt.close();
    assertTrue(stmt.isClosed());

  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    Statement stmt = conn.createStatement();
    assertThat(stmt.execute(""), is(false));
    assertThat(stmt.getResultSet(), is(nullValue()));
    assertTrue(!stmt.getMoreResults());
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {

    Statement st = conn.createStatement();
    assertThat(st.execute("select 1"), is(true));
    assertNotNull(st.getResultSet());
    ResultSet resultSet = st.getResultSet();
    resultSet.first();
    int one = resultSet.getInt("1");
    assertThat(one, is(1));
    assertThat(st.getMoreResults(), is(false));

  }

}
