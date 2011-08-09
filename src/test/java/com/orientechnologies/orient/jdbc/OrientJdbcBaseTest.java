package com.orientechnologies.orient.jdbc;

import org.junit.After;
import org.junit.Before;

import static java.lang.Class.forName;

public abstract class OrientJdbcBaseTest {

	protected OrientJdbcConnection conn;

	@Before
	public void loadDriver() throws Exception {
		forName(OrientJdbcDriver.class.getName());
	}

	@After
	public void closeConnection() throws Exception {
		if (!conn.isClosed()) conn.close();
	}

}
