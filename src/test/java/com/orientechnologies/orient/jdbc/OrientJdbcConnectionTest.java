package com.orientechnologies.orient.jdbc;

import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

public class OrientJdbcConnectionTest extends OrientJdbcBaseTest {

	@Before
	public void setUp() throws Exception {
		String dbUrl = "local:./working/db/test";
		createDB(dbUrl);
		loadDB(dbUrl, 20);
	}

	@Test
	public void shouldCreateStatement() throws Exception {
		Statement stmt = conn.createStatement();
		assertNotNull(stmt);
		stmt.close();
	}

	@Test
	public void checkSomePrecondition() throws Exception {

		assertFalse(conn.isClosed());
		conn.isReadOnly();

		conn.isValid(0);
		conn.setAutoCommit(true);
		assertTrue(conn.getAutoCommit());
		// conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
		// assertEquals(Connection.TRANSACTION_NONE,
		// conn.getTransactionIsolation());
	}

}
