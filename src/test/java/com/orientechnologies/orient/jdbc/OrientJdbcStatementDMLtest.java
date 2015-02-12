package com.orientechnologies.orient.jdbc;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class OrientJdbcStatementDMLtest extends OrientJdbcBaseTest {

  @Test
  public void shouldInsertANewItem() throws Exception {

    assertFalse(conn.isClosed());
    Date date = new Date(System.currentTimeMillis());

    Statement stmt = conn.createStatement();
    int updated = stmt
        .executeUpdate("INSERT into Item (stringKey, intKey, text, length, date) values ('100','100','dummy text','10','"
            + date.toString() + "')");

    assertEquals(1, updated);

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '100' ");
    rs.next();
    assertEquals(100, rs.getInt("intKey"));
    assertEquals("100", rs.getString("stringKey"));
    assertEquals(date.toString(), rs.getDate("date").toString());

  }

  @Test
  public void shouldUpdateAnItem() throws Exception {

    assertFalse(conn.isClosed());

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("UPDATE Item set text = 'UPDATED'  WHERE intKey = '10'");

    assertFalse(stmt.getMoreResults());
    assertEquals(1, updated);

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    rs.next();
    assertEquals("UPDATED", rs.getString("text"));

  }

  @Test
  public void shouldDeleteAnItem() throws Exception {

    assertFalse(conn.isClosed());

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("DELETE FROM Item WHERE intKey = '10'");

    assertFalse(stmt.getMoreResults());
    assertEquals(1, updated);

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    assertFalse(rs.next());

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
    ODatabaseDocumentTx database = conn.getDatabase();
    assertThat(database.getClusterIdByName("account"), notNullValue());
    OClass account = database.getMetadata().getSchema().getClass("Account");
    assertThat(account, notNullValue());
    assertThat(account.getProperty("id").getType(), equalTo(OType.INTEGER));
    assertThat(account.getProperty("birthDate").getType(), equalTo(OType.DATE));
    assertThat(account.getProperty("binary").getType(), equalTo(OType.BINARY));

  }

  @Test
  public void shoulCreateClassWithBatchCommand() throws IOException, SQLException {

    Statement stmt = conn.createStatement();

    stmt.addBatch("CREATE CLASS Account ");
    stmt.addBatch("CREATE PROPERTY Account.id INTEGER ");
    stmt.addBatch("CREATE PROPERTY Account.birthDate DATE ");
    stmt.addBatch("CREATE PROPERTY Account.binary BINARY ");
    int[] results = stmt.executeBatch();
    assertThat(results.length, equalTo(4));
    stmt.close();

    // double value test pattern?
    ODatabaseDocumentTx database = conn.getDatabase();
    assertThat(database.getClusterIdByName("account"), notNullValue());
    OClass account = database.getMetadata().getSchema().getClass("Account");
    assertThat(account, notNullValue());
    assertThat(account.getProperty("id").getType(), equalTo(OType.INTEGER));
    assertThat(account.getProperty("birthDate").getType(), equalTo(OType.DATE));
    assertThat(account.getProperty("binary").getType(), equalTo(OType.BINARY));

  }

}
