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
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;

public class OrientJdbcPreparedStatementTest extends OrientJdbcDbPerMethodTemplateTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    PreparedStatement stmt =
        conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
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
    ResultSet result =
        conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1) return @rid");

    assertThat(result.next()).isTrue();
    assertThat(result.getObject("@rid")).isNotNull();
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
    PreparedStatement stmt =
        conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");

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
    // assertThat(rs.getDate("date").toString(), equalTo(new
    // java.sql.Date(System.currentTimeMillis()).toString()));
    // assertThat(rs.getDate("time").toString(), equalTo(new
    // java.sql.Date(System.currentTimeMillis()).toString()));

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
    stmt.close();

    // Let's verify the previous process
    ResultSet resultSet =
        conn.createStatement()
            .executeQuery("SELECT count(*) AS num FROM insertable WHERE id = 'someRandomUid'");
    assertThat(resultSet.getLong(1)).isEqualTo(1);

    // without alias!
    resultSet =
        conn.createStatement()
            .executeQuery("SELECT count(*) FROM insertable WHERE id = 'someRandomUid'");
    assertThat(resultSet.getLong(1)).isEqualTo(1);
  }

  @Test
  public void shouldCreatePreparedStatementWithExtendConstructor() throws Exception {

    PreparedStatement stmt =
        conn.prepareStatement(
            "SELECT * FROM Item WHERE intKey = ?", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    stmt.setInt(1, 1);

    ResultSet rs = stmt.executeQuery();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
  }

  @Test
  public void shouldCreatePreparedStatementWithExtendConstructorWithOutProjection()
      throws Exception {
    // same test as above, no projection at all
    PreparedStatement stmt =
        conn.prepareStatement(
            "SELECT FROM Item WHERE intKey = ?", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    stmt.setInt(1, 1);

    ResultSet rs = stmt.executeQuery();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
  }

  @Test(expected = SQLException.class)
  public void shouldThrowSqlExceptionOnError() throws SQLException {

    String query = "select sequence('?').next()";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, "theSequence");
    stmt.executeQuery();
  }
}
