package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcPreparedStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
    assertThat(stmt).isNotNull();
    stmt.close();
    assertThat(stmt.isClosed()).isTrue();

  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("");
    assertThat(stmt.execute("")).isFalse();

    assertThat(stmt.getResultSet()).isNull();
    assertThat(stmt.getMoreResults()).isFalse();
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select 1");
    assertThat(stmt.execute()).isTrue();
    assertThat(stmt.getResultSet()).isNotNull();
    ResultSet resultSet = stmt.getResultSet();
    resultSet.first();
    int one = resultSet.getInt("1");
    assertThat(one).isEqualTo(1);
    assertThat(stmt.getMoreResults()).isFalse();

  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");

    PreparedStatement statement = conn.prepareStatement("INSERT INTO Insertable ( id ) VALUES (?)");
    statement.setString(1, "testval");
    int rowsInserted = statement.executeUpdate();

    assertThat(rowsInserted).isEqualTo(1);
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInsertedWhenMultipleInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");

    PreparedStatement statement = conn.prepareStatement("UPDATE Insertable SET id = ?");
    statement.setString(1, "testval");
    int rowsInserted = statement.executeUpdate();

    assertThat(rowsInserted).isEqualTo(2);
  }


  @Test
  public void testInsertRIDReturning() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    ResultSet result = conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1) return @rid");

    assertThat(result.next()).isTrue();
    assertThat(result.getObject("id")).isNotNull();
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsDeleted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");

    PreparedStatement statement = conn.prepareStatement("DELETE FROM Insertable WHERE id > ?");
    statement.setInt(1, 0);
    int rowsDeleted = statement.executeUpdate();

    assertThat(rowsDeleted).isEqualTo(2);
  }

  @Test
  public void shouldExecutePreparedStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");

    assertThat(stmt).isNotNull();
    stmt.setString(1, "1");
    stmt.setInt(2, 1);

    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next()).isTrue();

    // assertThat(rs.getInt("@version"), equalTo(0));

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
    // assertThat(rs.getDate("date").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));
    // assertThat(rs.getDate("time").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));

    stmt.close();
    assertThat(stmt.isClosed()).isTrue();

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
    assertThat(resultSet.getInt(1)).isEqualTo(1);
  }

  @Test
  public void shouldCreatePreparedStatementWithExtendConstructor() throws Exception {

    PreparedStatement stmt = conn.prepareStatement("SELECT FROM Item WHERE intKey = ?", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    stmt.setInt(1, 1);

    ResultSet rs = stmt.executeQuery();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
  }

  @Test(expected = SQLException.class)
  public void shouldTrhowSqlExceptionOnError() throws SQLException {

    String query = "select sequence('?').next()";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, "theSequence");
    stmt.executeQuery();

  }
}
