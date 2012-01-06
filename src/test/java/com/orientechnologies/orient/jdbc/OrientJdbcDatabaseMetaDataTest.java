package com.orientechnologies.orient.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.Test;

import com.orientechnologies.orient.core.OConstants;

import static org.junit.Assert.assertEquals;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

	@Test
	public void shouldGiveMetadataAboutDatabase() throws SQLException {
		DatabaseMetaData metaData = conn.getMetaData();

		assertEquals("memory:test", metaData.getURL());
		assertEquals("admin", metaData.getUserName());
		assertEquals("OrientDB", metaData.getDatabaseProductName());
		assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
		assertEquals(1, metaData.getDatabaseMajorVersion());
		assertEquals(0, metaData.getDatabaseMinorVersion());

		assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
		assertEquals("OrientDB 1.0 JDBC Driver", metaData.getDriverVersion());
		assertEquals(1, metaData.getDriverMajorVersion());
		assertEquals(0, metaData.getDriverMinorVersion());

	}

}
