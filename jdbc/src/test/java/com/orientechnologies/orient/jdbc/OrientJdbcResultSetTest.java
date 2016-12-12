/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://orientdb.com
 */
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
    db.command(new OCommandSQL("INSERT INTO Article(uuid, date, title, content) VALUES (123456, null, 'title', 'the content')"))
        .execute();

    List<ODocument> docs = db.query(new OSQLSynchQuery<>("SELECT uuid,date, title, content FROM Article WHERE uuid = 123456"));

    //    docs.stream().forEach(d -> System.out.println(d.toJSON()));

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT uuid,date, title, content FROM Article WHERE uuid = 123456")).isTrue();
    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();

    assertThat(rs.getFetchSize()).isEqualTo(1);

    rs.getLong("uuid");
    rs.getDate(2);

  }

  @Test
  public void shouldSelectContentInsertedByInsertContent() throws Exception {

    Statement insert = conn.createStatement();
    insert.execute("INSERT INTO Article CONTENT {'uuid':'1234567',  'title':'title', 'content':'content'} ");

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT uuid,date, title, content FROM Article WHERE uuid = 1234567")).isTrue();

    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();

    assertThat(rs.getFetchSize()).isEqualTo(1);

    assertThat(rs.getLong("uuid")).isEqualTo(1234567);

  }

  @Test
  public void shouldSelectWithDistinct() throws Exception {

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT DISTINCT(published) as pub FROM Item ")).isTrue();

    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();

    assertThat(rs.getFetchSize()).isEqualTo(2);

    assertThat(rs.getBoolean(1)).isEqualTo(true);
    assertThat(rs.getBoolean("pub")).isEqualTo(true);

  }

  @Test
  public void shouldSelectWithSum() throws Exception {

    Statement stmt = conn.createStatement();

    assertThat(stmt.execute("SELECT sum(score) as totalScore FROM Item ")).isTrue();

    ResultSet rs = stmt.getResultSet();
    assertThat(rs).isNotNull();

    assertThat(rs.getFetchSize()).isEqualTo(1);

    assertThat(rs.getLong(1)).isEqualTo(3438);
    assertThat(rs.getLong("totalScore")).isEqualTo(3438);

  }

}
