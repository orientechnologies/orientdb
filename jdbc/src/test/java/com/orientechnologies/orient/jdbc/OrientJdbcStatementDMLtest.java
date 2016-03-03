package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class OrientJdbcStatementDMLtest extends OrientJdbcBaseTest {

  @Test
  public void shouldInsertANewItem() throws Exception {

    Date date = new Date(System.currentTimeMillis());

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate(
        "INSERT into Item (stringKey, intKey, text, length, date,score) values ('100','100','dummy text','10','" + date.toString()
            + "','10.10')");

    assertThat(updated, equalTo(1));

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date, score FROM Item where intKey = '100' ");
    rs.next();
    assertThat(rs.getInt("intKey"), equalTo(100));
    assertThat(rs.getString("stringKey"), equalTo("100"));
    assertThat(rs.getDate("date").toString(), equalTo(date.toString()));
    assertThat(rs.getBigDecimal("score"), equalTo(BigDecimal.valueOf(1010, 2)));

  }

  @Test
  public void shouldUpdateAnItem() throws Exception {

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("UPDATE Item set text = 'UPDATED'  WHERE intKey = '10'");

    assertThat(stmt.getMoreResults(), is(false));
    assertThat(updated, equalTo(1));

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    rs.next();
    assertThat(rs.getString("text"), equalTo("UPDATED"));

  }

  @Test
  public void shouldDeleteAnItem() throws Exception {

    Statement stmt = conn.createStatement();
    int updated = stmt.executeUpdate("DELETE FROM Item WHERE intKey = '10'");

    assertThat(stmt.getMoreResults(), is(false));
    assertEquals(1, updated);

    stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
    assertThat(rs.next(), is(false));

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
    assertThat(stmt.executeBatch().length, equalTo(4));
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
