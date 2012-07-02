package com.orientechnologies.orient.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.OConstants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

	private DatabaseMetaData metaData;

	@Before
	public void setup() throws SQLException {
		metaData = conn.getMetaData();

	}

	@Test
	public void verifyDriverAndDatabaseVersions() throws SQLException {

		assertEquals("memory:test", metaData.getURL());
		assertEquals("admin", metaData.getUserName());
		assertEquals("OrientDB", metaData.getDatabaseProductName());
		assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
		assertEquals(1, metaData.getDatabaseMajorVersion());
		assertEquals(1, metaData.getDatabaseMinorVersion());

		assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
		assertEquals("OrientDB 1.0 JDBC Driver", metaData.getDriverVersion());
		assertEquals(1, metaData.getDriverMajorVersion());
		assertEquals(0, metaData.getDriverMinorVersion());

	}

	@Test
	public void shouldRetrievePrimaryKeysMetadata() throws SQLException {

		ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "Item");
		assertTrue(primaryKeys.next());
		assertEquals("intKey", primaryKeys.getString(4));
		assertEquals("Item.intKey", primaryKeys.getString(6));
		assertEquals(1, primaryKeys.getInt(5));

		assertTrue(primaryKeys.next());
		assertEquals("stringKey", primaryKeys.getString("COLUMN_NAME"));
		assertEquals("Item.stringKey", primaryKeys.getString("PK_NAME"));
		assertEquals(1, primaryKeys.getInt("KEY_SEQ"));

	}

	@Test
	public void shouldRetrieveTableTypes() throws SQLException {

		ResultSet tableTypes = metaData.getTableTypes();
		assertTrue(tableTypes.next());
		assertEquals("TABLE", tableTypes.getString(1));

		assertFalse(tableTypes.next());

	}

}
