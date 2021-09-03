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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class OrientJdbcStatementDMLtest extends OrientJdbcDbPerMethodTemplateTest {

  @Test
  public void shouldInsertANewItem() throws Exception {

    Date date = new Date(System.currentTimeMillis());

    Statement stmt = conn.createStatement();
    int updated =
        stmt.executeUpdate(
            "INSERT into Item (stringKey, intKey, text, length, date) values ('100','100','dummy text','10','"
                + date.toString()
                + "')");

    assertThat(updated).isEqualTo(1);

    stmt = conn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "SELECT stringKey, intKey, text, length, date FROM Item where intKey = '100' ");
    rs.next();
    assertThat(rs.getInt("intKey")).isEqualTo(100);
    assertThat(rs.getString("stringKey")).isEqualTo("100");
    assertThat(rs.getDate("date").toString()).isEqualTo(date.toString());
  }

  @Test
  public void shouldUpdateAnItem() throws Exception {

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("UPDATE Item set text = 'UPDATED'  WHERE intKey = '10'");

    assertThat(stmt.getMoreResults()).isFalse();
    assertThat(updated).isEqualTo(1);

    stmt = conn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    rs.next();
    assertThat(rs.getString("text")).isEqualTo("UPDATED");
  }

  @Test
  public void shouldDeleteAnItem() throws Exception {

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("DELETE FROM Item WHERE intKey = '10'");

    assertThat(stmt.getMoreResults()).isFalse();
    assertThat(updated).isEqualTo(1);

    stmt = conn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    assertThat(rs.next()).isFalse();
  }

  @Test
  public void shoulCreateClassWithProperties() throws IOException, SQLException {

    Statement stmt = conn.createStatement();

    stmt.executeUpdate("CREATE CLASS Account ");
    stmt.executeUpdate("CREATE PROPERTY Account.id INTEGER ");
    stmt.executeUpdate("CREATE PROPERTY Account.birthDate DATE ");
    stmt.executeUpdate("CREATE PROPERTY Account.binary BINARY ");
    stmt.close();

    // double value test pattern?
    ODatabaseDocument database = conn.getDatabase();
    assertThat(database.getClusterIdByName("account")).isNotNull();
    OClass account = database.getMetadata().getSchema().getClass("Account");
    assertThat(account).isNotNull();
    assertThat(account.getProperty("id").getType()).isEqualTo(OType.INTEGER);
    assertThat(account.getProperty("birthDate").getType()).isEqualTo(OType.DATE);
    assertThat(account.getProperty("binary").getType()).isEqualTo(OType.BINARY);
  }

  @Test
  public void shouldCreateClassWithBatchCommand() throws IOException, SQLException {

    Statement stmt = conn.createStatement();

    stmt.addBatch("CREATE CLASS Account ");
    stmt.addBatch("CREATE PROPERTY Account.id INTEGER ");
    stmt.addBatch("CREATE PROPERTY Account.birthDate DATE ");
    stmt.addBatch("CREATE PROPERTY Account.binary BINARY ");
    assertThat(stmt.executeBatch()).hasSize(4);
    stmt.close();

    // double value test pattern?
    ODatabaseDocument database = conn.getDatabase();
    assertThat(database.getClusterIdByName("account")).isNotNull();
    OClass account = database.getMetadata().getSchema().getClass("Account");
    assertThat(account).isNotNull();
    assertThat(account.getProperty("id").getType()).isEqualTo(OType.INTEGER);
    assertThat(account.getProperty("birthDate").getType()).isEqualTo(OType.DATE);
    assertThat(account.getProperty("binary").getType()).isEqualTo(OType.BINARY);
  }
}
