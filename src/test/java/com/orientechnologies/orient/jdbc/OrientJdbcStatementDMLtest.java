package com.orientechnologies.orient.jdbc;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OrientJdbcStatementDMLtest extends OrientJdbcBaseTest {

	@Test
	public void shouldCreateANewRow() throws Exception {

		assertFalse(conn.isClosed());
		Date date = new Date(System.currentTimeMillis());

		Statement stmt = conn.createStatement();
		int updated = stmt.executeUpdate("INSERT into Item (stringKey, intKey, text, length, date) values ('100','100','dummy text','10','" + date.toString() + "')");

		assertEquals(1, updated);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '100' ");
		rs.next();
		assertEquals(100, rs.getInt("intKey"));
		assertEquals("100", rs.getString("stringKey"));
		assertEquals(date.toString(), rs.getDate("date").toString());

	}

	@Test
	public void shouldUpdateARow() throws Exception {

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
	public void shouldDeleteArow() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		int updated = stmt.executeUpdate("DELETE FROM Item WHERE intKey = '10'");

		assertFalse(stmt.getMoreResults());
		assertEquals(1, updated);

		stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item where intKey = '10' ");
		assertFalse(rs.next());

	}

}
