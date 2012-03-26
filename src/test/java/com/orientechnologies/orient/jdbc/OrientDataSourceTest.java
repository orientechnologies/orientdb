package com.orientechnologies.orient.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrientDataSourceTest {

	@Test
	public void shouldConnect() throws SQLException {
		String dbUrl = "memory:test";

		String username = "admin";
		String password = "admin";

		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
		if (db.exists()) {
			db.open(username, password);
			db.drop();
			db.close();
		}

		db.create();

		OrientDataSource ds = new OrientDataSource();
		ds.setUrl("jdbc:orient:memory:test");
		ds.setUsername(username);
		ds.setPassword(password);

		Connection conn = ds.getConnection();

		assertNotNull(conn);
		conn.close();
		assertTrue(conn.isClosed());

	}

}
