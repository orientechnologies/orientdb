/*
 * Copyright 2011 TXT e-solutions SpA
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * This work was performed within the IoT_at_Work Project
 * and partially funded by the European Commission's
 * 7th Framework Programme under the research area ICT-2009.1.3
 * Internet of Things and enterprise environments.
 *
 * Authors:
 *      Salvatore Piccione (TXT e-solutions SpA)
 *
 * Contributors:
 *        Domenico Rotondi (TXT e-solutions SpA)
 */
package com.orientechnologies.orient.jdbc.graph;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import com.orientechnologies.orient.jdbc.OrientJdbcDriver;

import static java.lang.Class.forName;

/**
 * @author Salvatore Piccione
 */
public abstract class OrientGraphJdbcBaseTest {

	private static final String DB_URL_PREFIX = "jdbc:orient:";

	private static final Properties info = new Properties();

	protected OrientJdbcConnection conn;

	@BeforeClass
	public static void setUpClass() {

		// create the graph
		OrientGraphJdbcCreationHelper.createGraphDatabase();

		info.put("user", OrientGraphJdbcCreationHelper.USERNAME);
		info.put("password", OrientGraphJdbcCreationHelper.PASSWORD);

		// load the JDBC driver
		try {
			forName(OrientJdbcDriver.class.getName());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An error occured during the setting up of the test classes: " + e.getMessage());
		}
	}

	@Before
	public void setUp() {
		try {
			conn = (OrientJdbcConnection) DriverManager.getConnection(DB_URL_PREFIX + OrientGraphJdbcCreationHelper.URL_DB, info);
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the set up of the JDBC connection: " + e.getMessage());
		}
	}

	@After
	public void tearDown() {
		try {
			if (!conn.isClosed()) conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("An error occured during the closing of the JDBC connection: " + e.getMessage());
		}
	}
}
