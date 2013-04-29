package com.orientechnologies.orient.jdbc;

import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

public class OrientJdbcConnectionTest extends OrientJdbcBaseTest {

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

	@Test
	public void shouldCreateDifferentTypeOfStatement() throws Exception {
		Statement stmt = conn.createStatement();
		assertNotNull(stmt);

		stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
		assertNotNull(stmt);

		stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
		assertNotNull(stmt);

	}

}
