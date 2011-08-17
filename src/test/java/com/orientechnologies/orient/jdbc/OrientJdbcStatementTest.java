package com.orientechnologies.orient.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OrientJdbcStatementTest extends OrientJdbcBaseTest {

	@Test
	public void shouldCreateStatement() throws Exception {
		Statement stmt = conn.createStatement();
		assertNotNull(stmt);
		stmt.close();
		assertTrue(stmt.isClosed());

	}

	@Test
	public void testEmptyQuery() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.execute("");
		assertNull(stmt.getResultSet());
		assertTrue(!stmt.getMoreResults());
	}

}
