/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
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
 */
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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import static java.util.Arrays.binarySearch;

public class OrientJdbcResultSet implements ResultSet {
	private List<ODocument> records = null;
	private OrientJdbcStatement statement;

	private int cursor = -1;
	private int rowCount = 0;
	private ODocument document;
	private String[] fieldNames;
	private final OrientJdbcConnection connection;

	public OrientJdbcResultSet(OrientJdbcConnection connection, OrientJdbcStatement iOrientJdbcStatement, List<ODocument> iRecords) {
		this.connection = connection;
		statement = iOrientJdbcStatement;
		records = iRecords;
		rowCount = iRecords.size();
	}

	public void close() throws SQLException {
		cursor = 0;
		rowCount = 0;
		records = null;
	}

	public boolean first() throws SQLException {
		return absolute(0);
	}

	public boolean last() throws SQLException {
		return absolute(rowCount - 1);
	}

	public boolean next() throws SQLException {
		return absolute(++cursor);
	}

	public boolean previous() throws SQLException {
		return absolute(++cursor);
	}

	public void afterLast() throws SQLException {
		// OUT OF LAST ITEM
		cursor = rowCount;
	}

	public void beforeFirst() throws SQLException {
		// OUT OF FIRST ITEM
		cursor = -1;
	}

	public boolean relative(int iRows) throws SQLException {
		return absolute(cursor + iRows);
	}

	public boolean absolute(int iRowNumber) throws SQLException {
		if (iRowNumber > rowCount - 1) {
			// OUT OF LAST ITEM
			cursor = rowCount;
			return false;
		} else if (iRowNumber < 0) {
			// OUT OF FIRST ITEM
			cursor = -1;
			return false;
		}

		cursor = iRowNumber;
		document = records.get(cursor);

		fieldNames = document.fieldNames();
		return true;
	}

	public boolean isAfterLast() throws SQLException {
		return cursor >= rowCount - 1;
	}

	public boolean isBeforeFirst() throws SQLException {
		return cursor < 0;
	}

	public boolean isClosed() throws SQLException {
		return records == null;
	}

	public boolean isFirst() throws SQLException {
		return cursor == 0;
	}

	public boolean isLast() throws SQLException {
		return cursor == rowCount - 1;
	}

	public Statement getStatement() throws SQLException {
		return statement;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return new OrientJdbcMetaData(connection, this);
	}

	public void deleteRow() throws SQLException {
		document.delete();
	}

	public int findColumn(String columnLabel) throws SQLException {

		return binarySearch(fieldNames, 0, fieldNames.length, columnLabel) + 1;
	}

	public Array getArray(int columnIndex) throws SQLException {
		return null;
	}

	public Array getArray(String columnLabel) throws SQLException {
		return null;
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {

		return null;
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {

		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {

		return null;
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {

		return null;
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {

		return null;
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {

		return null;
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {

		return null;
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {

		return null;
	}

	public Blob getBlob(int columnIndex) throws SQLException {

		return null;
	}

	public Blob getBlob(String columnLabel) throws SQLException {

		return null;
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return getBoolean(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public boolean getBoolean(String columnLabel) throws SQLException {
		return (Boolean) document.field(columnLabel, OType.BOOLEAN);

	}

	public byte getByte(int columnIndex) throws SQLException {
		return getByte(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public byte getByte(String columnLabel) throws SQLException {
		return (Byte) document.field(columnLabel, OType.BYTE);
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return getBytes(fieldNames[columnIndex - 1]);
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		return document.field(columnLabel, OType.BINARY);
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {

		return null;
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {

		return null;
	}

	public Clob getClob(int columnIndex) throws SQLException {

		return null;
	}

	public Clob getClob(String columnLabel) throws SQLException {

		return null;
	}

	public int getConcurrency() throws SQLException {

		return 0;
	}

	public String getCursorName() throws SQLException {

		return null;
	}

	public Date getDate(int columnIndex) throws SQLException {
		return getDate(fieldNames[columnIndex - 1]);
	}

	public Date getDate(String columnLabel) throws SQLException {
		java.util.Date date = document.field(columnLabel, OType.DATETIME);
		return new Date(date.getTime());
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {

		return null;
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {

		return null;
	}

	public double getDouble(int columnIndex) throws SQLException {

		return getDouble(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public double getDouble(String columnLabel) throws SQLException {

		return (Double) document.field(columnLabel, OType.DOUBLE);
	}

	public int getFetchDirection() throws SQLException {

		return 0;
	}

	public int getFetchSize() throws SQLException {

		return rowCount;
	}

	public float getFloat(int columnIndex) throws SQLException {
		return getFloat(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public float getFloat(String columnLabel) throws SQLException {
		return (Float) document.field(columnLabel, OType.FLOAT);
	}

	public int getHoldability() throws SQLException {

		return 0;
	}

	public int getInt(int columnIndex) throws SQLException {

		return getInt(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public int getInt(String columnLabel) throws SQLException {

		return (Integer) document.field(columnLabel, OType.INTEGER);
	}

	public long getLong(int columnIndex) throws SQLException {
		return getLong(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public long getLong(String columnLabel) throws SQLException {

		return (Long) document.field(columnLabel, OType.LONG);
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {

		return null;
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {

		return null;
	}

	public NClob getNClob(int columnIndex) throws SQLException {

		return null;
	}

	public NClob getNClob(String columnLabel) throws SQLException {

		return null;
	}

	public String getNString(int columnIndex) throws SQLException {

		return null;
	}

	public String getNString(String columnLabel) throws SQLException {

		return null;
	}

	public Object getObject(int columnIndex) throws SQLException {

		return getObject(fieldNames[columnIndex - 1]);
	}

	public Object getObject(String columnLabel) throws SQLException {

		return document.field(columnLabel);
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {

		return null;
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {

		return null;
	}

	public Ref getRef(int columnIndex) throws SQLException {

		return null;
	}

	public Ref getRef(String columnLabel) throws SQLException {

		return null;
	}

	public int getRow() throws SQLException {

		return cursor;
	}

	public RowId getRowId(int columnIndex) throws SQLException {

		return null;
	}

	public RowId getRowId(String columnLabel) throws SQLException {

		return null;
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {

		return null;
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {

		return null;
	}

	public short getShort(int columnIndex) throws SQLException {

		return getShort(fieldNames[columnIndex - 1]);
	}

	@SuppressWarnings("boxing")
	public short getShort(String columnLabel) throws SQLException {

		return (Short) document.field(columnLabel, OType.SHORT);
	}

	public String getString(int columnIndex) throws SQLException {

		return getString(fieldNames[columnIndex - 1]);
	}

	public String getString(String columnLabel) throws SQLException {
		return document.field(columnLabel, OType.STRING);

	}

	public Time getTime(int columnIndex) throws SQLException {

		return getTime(fieldNames[columnIndex - 1]);
	}

	public Time getTime(String columnLabel) throws SQLException {
		java.util.Date date = document.field(columnLabel, OType.DATETIME);
		return new Time(date.getTime());
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {

		return null;
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {

		return null;
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {

		return null;
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {

		return null;
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {

		return null;
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {

		return null;
	}

	public int getType() throws SQLException {

		return 0;
	}

	public URL getURL(int columnIndex) throws SQLException {

		return null;
	}

	public URL getURL(String columnLabel) throws SQLException {

		return null;
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {

		return null;
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {

		return null;
	}

	public SQLWarning getWarnings() throws SQLException {

		return null;
	}

	public void insertRow() throws SQLException {

	}

	public void moveToCurrentRow() throws SQLException {

	}

	public void moveToInsertRow() throws SQLException {

	}

	public void refreshRow() throws SQLException {

	}

	public boolean rowDeleted() throws SQLException {

		return false;
	}

	public boolean rowInserted() throws SQLException {

		return false;
	}

	public boolean rowUpdated() throws SQLException {

		return false;
	}

	public void setFetchDirection(int direction) throws SQLException {

	}

	public void setFetchSize(int rows) throws SQLException {

	}

	public void updateArray(int columnIndex, Array x) throws SQLException {

	}

	public void updateArray(String columnLabel, Array x) throws SQLException {

	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {

	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {

	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {

	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {

	}

	public void updateByte(int columnIndex, byte x) throws SQLException {

	}

	public void updateByte(String columnLabel, byte x) throws SQLException {

	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {

	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {

	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {

	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {

	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {

	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {

	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

	}

	public void updateDate(int columnIndex, Date x) throws SQLException {

	}

	public void updateDate(String columnLabel, Date x) throws SQLException {

	}

	public void updateDouble(int columnIndex, double x) throws SQLException {

	}

	public void updateDouble(String columnLabel, double x) throws SQLException {

	}

	public void updateFloat(int columnIndex, float x) throws SQLException {

	}

	public void updateFloat(String columnLabel, float x) throws SQLException {

	}

	public void updateInt(int columnIndex, int x) throws SQLException {

	}

	public void updateInt(String columnLabel, int x) throws SQLException {

	}

	public void updateLong(int columnIndex, long x) throws SQLException {

	}

	public void updateLong(String columnLabel, long x) throws SQLException {

	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {

	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {

	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

	}

	public void updateNString(int columnIndex, String nString) throws SQLException {

	}

	public void updateNString(String columnLabel, String nString) throws SQLException {

	}

	public void updateNull(int columnIndex) throws SQLException {

	}

	public void updateNull(String columnLabel) throws SQLException {

	}

	public void updateObject(int columnIndex, Object x) throws SQLException {

	}

	public void updateObject(String columnLabel, Object x) throws SQLException {

	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {

	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {

	}

	public void updateRow() throws SQLException {

	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {

	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {

	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

	}

	public void updateShort(int columnIndex, short x) throws SQLException {

	}

	public void updateShort(String columnLabel, short x) throws SQLException {

	}

	public void updateString(int columnIndex, String x) throws SQLException {

	}

	public void updateString(String columnLabel, String x) throws SQLException {

	}

	public void updateTime(int columnIndex, Time x) throws SQLException {

	}

	public void updateTime(String columnLabel, Time x) throws SQLException {

	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

	}

	public boolean wasNull() throws SQLException {

		return false;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {

		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {

		return null;
	}

	public void cancelRowUpdates() throws SQLException {
	}

	public void clearWarnings() throws SQLException {
	}

	// not on iterface
	public List<ODocument> getRecords() {
		return records;
	}

	public ODocument getDocument() {
		return document;
	}

}
