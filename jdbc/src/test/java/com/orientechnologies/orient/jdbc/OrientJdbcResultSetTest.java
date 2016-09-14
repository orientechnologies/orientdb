package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
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

  @Test
  public void shouldReturnReultSetWithSparkStyle() throws Exception {

    //set spark "profile"

    conn.getInfo().setProperty("spark", "true");
    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery("select \"stringKey\",\"published\" from item");

    assertThat(rs.next()).isTrue();

  }

  @Test
  public void shouldReadRowWithNullValue() throws Exception {

    db.activateOnCurrentThread();
    db.command(new OCommandSQL("INSERT INTO Article(uuid,date, title, content) VALUES (123456, null, 'title', 'the content')"))
        .execute();

    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>("SELECT uuid,date, title, content FROM Article WHERE uuid = 123456"));

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT uuid,date, title, content FROM Article WHERE uuid = 123456")).isTrue();
    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();

    assertThat(rs.getFetchSize()).isEqualTo(1);

    rs.getLong("uuid");
    rs.getDate(2);

  }
}
