package com.orientechnologies.orient.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;

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
	public void a() throws Exception {
		ODatabaseDocumentPool pool1 = new ODatabaseDocumentPool();
		pool1.setup(2, 10);

		ODatabaseDocumentTx db1 = pool1.acquire("remote:ares/bookmarks", "admin", "admin");

		ODatabaseDocumentPool pool2 = new ODatabaseDocumentPool();
		pool2.setup(2, 10);

		ODatabaseDocumentTx db2 = pool2.acquire("remote:poseidone/omero_it", "admin", "admin");

		ODatabaseDocumentPool pool3 = new ODatabaseDocumentPool();
		pool3.setup(2, 10);

		ODatabaseDocumentTx db3 = pool3.acquire("remote:localhost/tar", "admin", "admin");

	}

	@Test
	public void b() throws Exception {

		ODatabaseDocumentTx db3 = new ODatabaseDocumentTx("remote:localhost/tar");
		db3.open("admin", "admin");
		db3.close();
		
		ODatabaseDocumentTx db2 = new ODatabaseDocumentTx("remote:poseidone/omero_it");
		db2.open("admin", "admin");
		db2.close();
		
		
		ODatabaseDocumentTx db1 = new ODatabaseDocumentTx("remote:ares/bookmarks");
		db1.open("admin", "admin");
		db1.close();
		


	}

	@Test
    public void multipleDBConnection() throws Exception {

            ODatabaseDocumentTx db1 = new ODatabaseDocumentTx("remote:localhost/tar");
            db1.open("admin", "admin");
            db1.close();
            
            ODatabaseDocumentTx db2 = new ODatabaseDocumentTx("remote:ares/bookmarks");
            db2.open("admin", "admin");
    } 
	
	@Test
	public void shouldConnectTo2Databases() throws SQLException {
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "admin");

		Connection conn = DriverManager.getConnection("jdbc:orient:remote:ares/bookmarks", info);

		Connection conn2 = DriverManager.getConnection("jdbc:orient:remote:poseidone/omero_it", info);

	}

	@Test
	public void testWithPool() {
		ODatabaseDocumentTx db1 = ODatabaseDocumentPool.global().acquire("remote:ares/bookmarks", "admin", "admin");

		db1.load(new ORecordId("5:10"));

		ODatabaseDocumentTx db2 = ODatabaseDocumentPool.global().acquire("remote:poseidone/omero_it", "admin", "admin");

		db2.load(new ORecordId("5:10"));
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

	@Test
	public void test2remoteDb() {

		OneDatabasePool pool2 = new OneDatabasePool("remote:ares/bookmarks", "admin", "admin");

		ODatabaseDocumentTx db2 = pool2.acquire();

		ODocument load = db2.load(new ORecordId("5:10"));

		assertNotNull(db2);

		OneDatabasePool pool1 = new OneDatabasePool("remote:poseidone/omero_it", "admin", "admin");

		ODatabaseDocumentTx db1 = pool1.acquire();

		assertNotNull(db1);
		load = db1.load(new ORecordId("5:10"));

		db1.close();

		db2.close();

	}

}

class OneDatabasePool {

	private final String dbUrl;
	private final String username;
	private final String password;

	public OneDatabasePool(String dbUrl, String username, String password) {
		this.dbUrl = dbUrl;
		this.username = username;
		this.password = password;
	}

	public ODatabaseDocumentTx acquire() {

		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
		// ODatabaseRecordThreadLocal.INSTANCE.set(db);

		if (db.isClosed()) {
			db.open(username, password);
		}

		return db;
	}
}
