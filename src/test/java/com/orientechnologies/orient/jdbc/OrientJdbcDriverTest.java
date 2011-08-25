package com.orientechnologies.orient.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrientJdbcDriverTest {

	@Test
	public void shouldAcceptsWellFormattedURLOnly() throws ClassNotFoundException, SQLException {

		Driver drv = new OrientJdbcDriver();

		assertTrue(drv.acceptsURL("jdbc:orient:local:./working/db/test"));

		assertFalse(drv.acceptsURL("local:./working/db/test"));
	}

	@Test
	public void shouldConnect() throws ClassNotFoundException, SQLException {
		String dbUrl = "memory:test";

		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
		db.create();

		OrientJdbcConnection conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:" + dbUrl);

		assertNotNull(conn);
		conn.close();
		assertTrue(conn.isClosed());
	}

}
