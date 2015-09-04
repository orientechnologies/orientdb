package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class OrientJdbcPreparedStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
    assertThat(stmt, is(notNullValue()));
    stmt.close();
    assertThat(stmt.isClosed(), is(true));

  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("");
    assertThat(stmt.execute(""), is(false));

    assertThat(stmt.getResultSet(), is(nullValue()));
    assertThat(stmt.getMoreResults(), is(false));
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select 1");
    assertThat(stmt.execute(), is(true));
    assertNotNull(stmt.getResultSet());
    ResultSet resultSet = stmt.getResultSet();
    resultSet.first();
    int one = resultSet.getInt("1");
    assertThat(one, is(1));
    assertThat(stmt.getMoreResults(), is(false));

  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");

    PreparedStatement statement = conn.prepareStatement("INSERT INTO Insertable ( id ) VALUES (?)");
    statement.setString(1, "testval");
    int rowsInserted = statement.executeUpdate();

    assertThat(rowsInserted, equalTo(1));
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInsertedWhenMultipleInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");

    PreparedStatement statement = conn.prepareStatement("UPDATE Insertable SET id = ?");
    statement.setString(1, "testval");
    int rowsInserted = statement.executeUpdate();

    assertThat(rowsInserted, equalTo(2));
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsDeleted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");

    PreparedStatement statement = conn.prepareStatement("DELETE FROM Insertable WHERE id > ?");
    statement.setInt(1, 0);
    int rowsDeleted = statement.executeUpdate();

    assertThat(rowsDeleted, equalTo(2));
  }

  @Test
  public void shouldExecutePreparedStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");
    assertNotNull(stmt);

    stmt.setString(1, "1");
    stmt.setInt(2, 1);

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), is(true));

    // assertThat(rs.getInt("@version"), equalTo(0));

    assertThat(rs.getString("@class"), equalTo("Item"));

    assertThat(rs.getString("stringKey"), equalTo("1"));
    assertThat(rs.getInt("intKey"), equalTo(1));
    //
    // assertThat(rs.getDate("date").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));
    // assertThat(rs.getDate("time").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));

    stmt.close();
    assertThat(stmt.isClosed(), is(true));

  }

  @Test
  public void shouldExecutePreparedStatementWithExecuteMethod() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS insertable");
    PreparedStatement stmt = conn.prepareStatement("INSERT INTO insertable SET id = ?, number = ?");
    stmt.setString(1, "someRandomUid");
    stmt.setInt(2, 42);
    stmt.execute();

    // Let's verify the previous process
    ResultSet resultSet = conn.createStatement().executeQuery("SELECT count(*) FROM insertable WHERE id = 'someRandomUid'");
    assertThat(resultSet.getInt(1), equalTo(1));
  }
}
