package com.orientechnologies.orient.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

  @Test
  public void shouldMapReturnTypes() throws Exception {

    assertFalse(conn.isClosed());

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

    ResultSetMetaData metaData = rs.getMetaData();

    assertNotNull(metaData);
  }

  @Test
  public void testDouble() throws Exception {
    assertFalse(conn.isClosed());

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM Author limit 10");
    int count = 0;
    while (rs.next()) {
      Double rating = rs.getDouble("rating");
      assertNotNull(rating);
      count++;
    }
    assertEquals(count, 10);
  }

  @Test
  public void testDoubleConversion() throws Exception {
    assertFalse(conn.isClosed());

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM Author limit 10");
    int count = 0;
    while (rs.next()) {
      Double rating = rs.getDouble("uuid");
      assertNotNull(rating);
      count++;
    }
    assertEquals(count, 10);
  }
}
