package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

  @Test
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
  public void shouldReturnEmptyResultSet() throws Exception {

    ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Author where false = true");

    assertThat(rs.next()).isFalse();
  }

  @Test
  public void shouldReturnResultSetAfterExecute() throws Exception {

    assertThat(conn.isClosed()).isFalse();

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT stringKey, intKey, text, length, date FROM Item")).isTrue();
    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();
    assertThat(rs.getFetchSize()).isEqualTo(20);

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
