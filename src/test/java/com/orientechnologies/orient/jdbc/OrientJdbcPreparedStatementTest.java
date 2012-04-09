package com.orientechnologies.orient.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OrientJdbcPreparedStatementTest extends OrientJdbcBaseTest {

    @Test
    public void shouldCreateStatement() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
        assertNotNull(stmt);
        stmt.close();
        assertTrue(stmt.isClosed());

    }

    @Test
    public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("");
        stmt.execute("");
        assertNull(stmt.getResultSet());
        assertTrue(!stmt.getMoreResults());
    }

    @Test
    public void shouldExecutePreparedStatement() throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT stringKey, intKey FROM Item WHERE stringKey = ? OR intKey = ?");
        assertNotNull(stmt);

        stmt.setString(1, "1");
        stmt.setInt(2, 1);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());

        assertThat(rs.getString("stringKey"), equalTo("1"));
        assertThat(rs.getInt("intKey"), equalTo(1));

        stmt.close();
        assertTrue(stmt.isClosed());

    }

}
