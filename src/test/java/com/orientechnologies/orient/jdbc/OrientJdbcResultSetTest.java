package com.orientechnologies.orient.jdbc;

import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

	@Before
	public void setUp() throws Exception {
		String dbUrl = "local:./working/db/test";
		createDB(dbUrl);
		loadDB(dbUrl, 20);

		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "admin");

		conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:local:./working/db/test", info);

	}

	@After
	public void tearDown() throws Exception {
		conn.close();
	}

	@Test
	public void shouldMapReturnTypes() throws Exception {

		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");
		assertEquals(20, rs.getFetchSize());

		assertTrue(rs.next());

		assertEquals(0, rs.getRow());

		assertEquals("1", rs.getString(1));
		assertEquals("1", rs.getString("stringKey"));

		assertEquals(1, rs.getInt(2));
		assertEquals(1, rs.getInt("intKey"));

		assertEquals(rs.getString("text").length(), rs.getDouble("length"));

		Date date = new Date(System.currentTimeMillis());
		assertEquals(date.toString(), rs.getDate("date").toString());

	}
}
