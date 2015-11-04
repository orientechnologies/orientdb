package com.orientechnologies.orient.jdbc;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Calendar;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcResultSetMetaDataTest extends OrientJdbcBaseTest {

  @Test
  @Ignore
  public void shouldMapReturnTypes() throws Exception {

    assertThat(conn.isClosed()).isFalse();

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");
    assertThat(rs.getFetchSize()).isEqualTo(20);

    assertThat(rs.isBeforeFirst()).isTrue();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getRow()).isEqualTo(0);

    assertThat(rs.getString(1)).isEqualTo("1");
    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.findColumn("stringKey")).isEqualTo(1);

    assertThat(rs.getInt(2)).isEqualTo(1);
    assertThat(rs.getInt("intKey")).isEqualTo(1);

    assertThat(rs.getString("text")).hasSize(rs.getInt("length"));

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.add(Calendar.HOUR_OF_DAY, -1);
    Date date = new Date(cal.getTimeInMillis());
    assertThat(rs.getDate("date").toString()).isEqualTo(date.toString());
    assertThat(rs.getDate(5).toString()).isEqualTo(date.toString());

    rs.last();

    assertThat(rs.getRow()).isEqualTo(19);
    rs.close();

    assertThat(rs.isClosed()).isTrue();
  }

  @Test
  @Ignore
  public void shouldNavigateResultSet() throws Exception {

    assertThat(conn.isClosed()).isFalse();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM Item");
    assertThat(rs.getFetchSize()).isEqualTo(20);

    assertThat(rs.isBeforeFirst()).isTrue();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getRow()).isEqualTo(0);

    rs.last();

    assertThat(rs.getRow()).isEqualTo(19);

    assertThat(rs.next()).isFalse();

    rs.afterLast();

    assertThat(rs.next()).isFalse();

    rs.close();

    assertThat(rs.isClosed()).isTrue();

    stmt.close();

    assertThat(stmt.isClosed()).isTrue();
  }

  @Test
  @Ignore
  public void shouldReturnResultSetAfterExecute() throws Exception {

    assertThat(conn.isClosed()).isFalse();

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT stringKey, intKey, text, length, date FROM Item")).isTrue();
    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();
    assertThat(rs.getFetchSize()).isEqualTo(20);

  }

  @Test
  @Ignore
  public void shouldNavigateResultSetByMetadata() throws Exception {

    assertThat(conn.isClosed()).isFalse();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

    rs.next();
    ResultSetMetaData metaData = rs.getMetaData();
    assertThat(metaData.getColumnCount()).isEqualTo(5);

    assertThat(metaData.getColumnName(1)).isEqualTo("stringKey");
    assertThat(rs.getObject(1)).isInstanceOf(String.class);

    assertThat(metaData.getColumnName(2)).isEqualTo("intKey");

    assertThat(metaData.getColumnName(3)).isEqualTo("text");
    assertThat(rs.getObject(3)).isInstanceOf(String.class);

    assertThat(metaData.getColumnName(4)).isEqualTo("length");

    assertThat(metaData.getColumnName(5)).isEqualTo("date");

  }

  @Test
  @Ignore
  public void shouldMapOrientTypesToJavaSQL() throws Exception {
    assertThat(conn.isClosed()).isFalse();

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

    rs.next();
    ResultSetMetaData metaData = rs.getMetaData();

    assertThat(metaData.getColumnCount()).isEqualTo(5);

    assertThat(metaData.getColumnType(2)).isEqualTo(java.sql.Types.INTEGER);

    assertThat(metaData.getColumnType(3)).isEqualTo(java.sql.Types.VARCHAR);
    assertThat(rs.getObject(3)).isInstanceOf(String.class);

    assertThat(metaData.getColumnType(4)).isEqualTo(java.sql.Types.BIGINT);

    assertThat(metaData.getColumnType(5)).isEqualTo(java.sql.Types.TIMESTAMP);
    assertThat(metaData.getColumnClassName(1)).isEqualTo(String.class.getName());
    assertThat(metaData.getColumnType(1)).isEqualTo(java.sql.Types.VARCHAR);

  }

}
