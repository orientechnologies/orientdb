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

	public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection) {
		super(iConnection);
	}

	public ResultSet executeQuery() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int executeUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setByte(int parameterIndex, byte x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setShort(int parameterIndex, short x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setInt(int parameterIndex, int x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setLong(int parameterIndex, long x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setString(int parameterIndex, String x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void clearParameters() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public boolean execute() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void addBatch() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNString(int parameterIndex, String value) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

}
