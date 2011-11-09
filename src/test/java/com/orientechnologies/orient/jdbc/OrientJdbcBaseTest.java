package com.orientechnologies.orient.jdbc;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import static java.lang.Class.forName;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

public abstract class OrientJdbcBaseTest {

	protected OrientJdbcConnection conn;

	@BeforeClass
	public static void loadDriver() throws ClassNotFoundException {
		forName(OrientJdbcDriver.class.getName());

	}

	@Before
	public void prepareDatabase() throws Exception {
		String dbUrl = "local:./working/db/test";
		dbUrl = "memory:test";
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
		createDB(db);
		loadDB(db, 20);

		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "admin");

		conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:" + dbUrl, info);

	}

	@After
	public void closeConnection() throws Exception {
		if (!conn.isClosed()) conn.close();
	}

}
