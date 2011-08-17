package com.orientechnologies.orient.jdbc;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

	@Test
	public void shouldMapReturnTypes() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");
		assertEquals(20, rs.getFetchSize());

		assertTrue(rs.isBeforeFirst());

		assertTrue(rs.next());

		assertEquals(0, rs.getRow());

		assertEquals("1", rs.getString(1));
		assertEquals("1", rs.getString("stringKey"));

		assertEquals(1, rs.getInt(2));
		assertEquals(1, rs.getInt("intKey"));

		assertEquals(rs.getString("text").length(), rs.getLong("length"));

		Date date = new Date(System.currentTimeMillis());
		assertEquals(date.toString(), rs.getDate("date").toString());

		rs.last();

		assertEquals(19, rs.getRow());
		rs.close();

		assertTrue(rs.isClosed());
	}

	@Test
	public void shouldNavigateResultSet() throws Exception {
		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM Item");
		assertEquals(20, rs.getFetchSize());

		assertTrue(rs.isBeforeFirst());

		assertTrue(rs.next());

		assertEquals(0, rs.getRow());

		rs.last();

		assertEquals(19, rs.getRow());

		assertFalse(rs.next());

		rs.afterLast();

		assertFalse(rs.next());

		rs.close();

		assertTrue(rs.isClosed());

		stmt.close();

		assertTrue(stmt.isClosed());
	}

	@Test
	public void shouldReturnResultSetAfterExecute() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();

		assertTrue(stmt.execute("SELECT stringKey, intKey, text, length, date FROM Item"));
		ResultSet rs = stmt.getResultSet();
		assertEquals(20, rs.getFetchSize());

	}

}
