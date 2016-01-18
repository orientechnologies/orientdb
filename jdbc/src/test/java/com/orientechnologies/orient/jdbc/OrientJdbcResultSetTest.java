package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

  @Test
  public void shouldMapReturnTypes() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

    ResultSetMetaData metaData = rs.getMetaData();
    assertThat(metaData).isNotNull();

    assertThat(metaData.getColumnType(1)).isEqualTo(Types.VARCHAR);
    assertThat(metaData.getColumnType(2)).isEqualTo(Types.INTEGER);
    assertThat(metaData.getColumnType(3)).isEqualTo(Types.VARCHAR);
    assertThat(metaData.getColumnType(4)).isEqualTo(Types.BIGINT);
    assertThat(metaData.getColumnType(5)).isEqualTo(Types.TIMESTAMP);
  }

  @Test
  public void shouldMapRatingToDouble() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author limit 10");
    int size = 0;
    while (rs.next()) {
      assertThat(rs.getDouble("rating")).isNotNull();
      size++;
    }
    assertThat(size).isEqualTo(10);
  }

  @Test
  public void shouldConvertUUIDToDouble() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author limit 10");
    int count = 0;
    while (rs.next()) {
      assertThat(rs.getDouble("uuid")).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(10);
  }

  @Test
  public void shouldReturnEmptyResultSet() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author where false = true");

    assertThat(rs.next()).isFalse();
  }
}
