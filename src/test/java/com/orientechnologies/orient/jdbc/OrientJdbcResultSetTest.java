package com.orientechnologies.orient.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

    @Test
    public void shouldMapReturnTypes() throws Exception {

        assertFalse(conn.isClosed());

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

        ResultSetMetaData metaData = rs.getMetaData();

        assertNotNull(metaData);
    }

}
