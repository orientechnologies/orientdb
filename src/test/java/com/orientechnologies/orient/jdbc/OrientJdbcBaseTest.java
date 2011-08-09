package com.orientechnologies.orient.jdbc;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import static java.lang.Class.forName;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

public abstract class OrientJdbcBaseTest {

	protected OrientJdbcConnection conn;

	@Before
	public void loadDriver() throws Exception {
		forName(OrientJdbcDriver.class.getName());
		String dbUrl = "local:./working/db/test";
		createDB(dbUrl);
		loadDB(dbUrl, 20);

		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "admin");

		conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:local:./working/db/test", info);

	}

	@After
	public void closeConnection() throws Exception {
		if (!conn.isClosed()) conn.close();
	}

}
