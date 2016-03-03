package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.nio.channels.Pipe;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

  @Test
  public void shouldMapReturnTypes() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT stringKey, intKey, text, length, date, score FROM Item");

    ResultSetMetaData metaData = rs.getMetaData();
    assertThat(metaData, is(notNullValue()));

    assertThat(metaData.getColumnType(1), equalTo(Types.VARCHAR));
    assertThat(metaData.getColumnType(2), equalTo(Types.INTEGER));
    assertThat(metaData.getColumnType(3), equalTo(Types.VARCHAR));
    assertThat(metaData.getColumnType(4), equalTo(Types.BIGINT));
    assertThat(metaData.getColumnType(5), equalTo(Types.TIMESTAMP));
    assertThat(metaData.getColumnType(6), equalTo(Types.DECIMAL));

  }

  @Test
  public void shouldMapRatingToDouble() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author limit 10");
    int size = 0;
    while (rs.next()) {
      assertThat(rs.getDouble("rating"), is(notNullValue()));
      size++;
    }
    assertEquals(size, 10);
  }

  @Test
  public void shouldConvertUUIDToDouble() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author limit 10");
    int count = 0;
    while (rs.next()) {
      assertThat(rs.getDouble("uuid"), is(notNullValue()));
      count++;
    }
    assertThat(10, equalTo(count));
  }

  @Test
  public void shouldReturnEmptyResultSet() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author where false = true");

    assertThat(rs.next(), is(false));
  }
}
