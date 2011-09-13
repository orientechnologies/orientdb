package com.orientechnologies.orient.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class OrientJdbcPreparedStatement extends OrientJdbcStatement implements PreparedStatement {

	public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql) {
		super(iConnection);
	}

	public ResultSet executeQuery() throws SQLException {
		return null;
	}

	public int executeUpdate() throws SQLException {
		return 0;
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {

	}

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {

	}

	public void setByte(int parameterIndex, byte x) throws SQLException {

	}

	public void setShort(int parameterIndex, short x) throws SQLException {

	}

	public void setInt(int parameterIndex, int x) throws SQLException {

	}

	public void setLong(int parameterIndex, long x) throws SQLException {

	}

	public void setFloat(int parameterIndex, float x) throws SQLException {

	}

	public void setDouble(int parameterIndex, double x) throws SQLException {

	}

	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

	}

	public void setString(int parameterIndex, String x) throws SQLException {

	}

	public void setBytes(int parameterIndex, byte[] x) throws SQLException {

	}

	public void setDate(int parameterIndex, Date x) throws SQLException {

	}

	public void setTime(int parameterIndex, Time x) throws SQLException {

	}

	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

	}

	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

	}

	public void clearParameters() throws SQLException {

	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

	}

	public void setObject(int parameterIndex, Object x) throws SQLException {

	}

	public boolean execute() throws SQLException {

		return false;
	}

	public void addBatch() throws SQLException {

	}

	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {

	}

	public void setBlob(int parameterIndex, Blob x) throws SQLException {

	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {

	}

	public void setArray(int parameterIndex, Array x) throws SQLException {

	}

	public ResultSetMetaData getMetaData() throws SQLException {

		return null;
	}

	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

	}

	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

	}

	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

	}

	public void setURL(int parameterIndex, URL x) throws SQLException {

	}

	public ParameterMetaData getParameterMetaData() throws SQLException {

		return null;
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {

	}

	public void setNString(int parameterIndex, String value) throws SQLException {

	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {

	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

	}

	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {

	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {

	}

}
