/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://orientdb.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OrientSql;
import com.orientechnologies.orient.core.sql.parser.ParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Roberto Franchini (CELI srl - franchin--at--celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci--at--gmail.com)
 */
public class OrientJdbcResultSet implements ResultSet {
  private final List<String>                fieldNames;
  private final OrientJdbcResultSetMetaData resultSetMetaData;
  private List<ODocument> records = null;
  private OrientJdbcStatement statement;
  private int cursor   = -1;
  private int rowCount = 0;
  private ODocument document;
  private int       type;
  private int       concurrency;
  private int       holdability;

  protected OrientJdbcResultSet(final OrientJdbcStatement statement,
      final List<ODocument> records,
      final int type,
      final int concurrency,
      int holdability) throws SQLException {

    this.statement = statement;
    this.records = records;
    rowCount = records.size();

    if (rowCount > 0) {
      document = (ODocument) records.get(0).getRecord();
    } else {
      document = new ODocument();
    }

    fieldNames = extractFieldNames(statement);

    activateDatabaseOnCurrentThread();
    if (type == TYPE_FORWARD_ONLY || type == TYPE_SCROLL_INSENSITIVE || type == TYPE_SCROLL_SENSITIVE)
      this.type = type;
    else
      throw new SQLException("Bad ResultSet type: " + type + " instead of one of the following values: " + TYPE_FORWARD_ONLY + ", "
          + TYPE_SCROLL_INSENSITIVE + " or" + TYPE_SCROLL_SENSITIVE);

    if (concurrency == CONCUR_READ_ONLY || concurrency == CONCUR_UPDATABLE)
      this.concurrency = concurrency;
    else
      throw new SQLException(
          "Bad ResultSet Concurrency type: " + concurrency + " instead of one of the following values: " + CONCUR_READ_ONLY + " or"
              + CONCUR_UPDATABLE);

    if (holdability == HOLD_CURSORS_OVER_COMMIT || holdability == CLOSE_CURSORS_AT_COMMIT)
      this.holdability = holdability;
    else
      throw new SQLException(
          "Bad ResultSet Holdability type: " + holdability + " instead of one of the following values: " + HOLD_CURSORS_OVER_COMMIT
              + " or" + CLOSE_CURSORS_AT_COMMIT);

    resultSetMetaData = new OrientJdbcResultSetMetaData(this);
  }

  private List<String> extractFieldNames(OrientJdbcStatement statement) {
    List<String> fields = new ArrayList<>();
    if (statement.sql != null && !statement.sql.isEmpty()) {
      try {
        OrientSql osql = new OrientSql(new ByteArrayInputStream(statement.sql.getBytes()));

        final OSelectStatement select = osql.SelectStatement();
        if (select.getProjection() != null) {
          fields.addAll(select.getProjection()
              .getItems()
              .stream()
              .filter(i -> !i.isAggregate())
              .filter(i -> !i.isAll())
              .map(i -> i.getProjectionAliasAsString())
              .collect(Collectors.toList()));
        }

        if (fields.size() == 1 && fields.contains("*")) {

          fields.clear();
        }
      } catch (ParseException e) {
        //NOOP
      }
    }
    if (fields.isEmpty()) {
      fields.addAll(Arrays.asList(document.fieldNames()));
    }
    return fields;
  }

  private void activateDatabaseOnCurrentThread() {
    statement.database.activateOnCurrentThread();
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
    document = (ODocument) records.get(cursor).getRecord();
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
    return resultSetMetaData;
  }

  public void deleteRow() throws SQLException {
    document.delete();
  }

  public int findColumn(String columnLabel) throws SQLException {
    int column = 0;
    int i = 0;
    while (i < (fieldNames.size() - 1) && column == 0) {
      if (fieldNames.get(i).equals(columnLabel))
        column = i + 1;
      else
        i++;
    }
    if (column == 0)
      throw new SQLException("The column '" + columnLabel + "' does not exists (Result Set element: " + rowCount + ")");
    return column;
  }

  private int getFieldIndex(final int columnIndex) throws SQLException {
    if (columnIndex < 1)
      throw new SQLException("The column index cannot be less than 1");
    return columnIndex - 1;
  }

  public Array getArray(int columnIndex) throws SQLException {
    return getArray(fieldNames.get(getFieldIndex(columnIndex)));

  }

  public Array getArray(String columnLabel) throws SQLException {

    OType columnType = document.fieldType(columnLabel);

    assert columnType.isEmbedded() && columnType.isMultiValue();

//    System.out.println("columnType.name() = " + columnType.getDefaultJavaType());

    OType.EMBEDDEDLIST.getDefaultJavaType();
    Array array = new OrientJdbcArray(document.field(columnLabel));

    return array;
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return null;
  }

  public InputStream getAsciiStream(final String columnLabel) throws SQLException {
    return null;
  }

  public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {

    return getBigDecimal(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    try {
      return (BigDecimal) document.field(columnLabel, OType.DECIMAL);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the double value at column '" + columnLabel + "'", e);
    }
  }

  public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    return getBigDecimal(fieldNames.get(getFieldIndex(columnIndex)), scale);
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    try {
      return ((BigDecimal) document.field(columnLabel, OType.DECIMAL)).setScale(scale);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the double value at column '" + columnLabel + "'", e);
    }
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return getBinaryStream(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    try {
      Blob blob = getBlob(columnLabel);
      return blob != null ? blob.getBinaryStream() : null;
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the binary stream at column '" + columnLabel + "'", e);
    }
  }

  public Blob getBlob(int columnIndex) throws SQLException {
    return getBlob(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public Blob getBlob(String columnLabel) throws SQLException {

    try {
      Object value = document.field(columnLabel);

      if (value instanceof OBlob) {
        return new OrientBlob((OBlob) value);
      } else if (value instanceof ORecordLazyList) {
        ORecordLazyList list = (ORecordLazyList) value;
        // check if all the list items are instances of ORecordBytes
        ListIterator<OIdentifiable> iterator = list.listIterator();

        List<OBlob> binaryRecordList = new ArrayList<OBlob>(list.size());
        while (iterator.hasNext()) {
          OIdentifiable listElement = iterator.next();

          OBlob ob = document.getDatabase().load(listElement.getIdentity());

          binaryRecordList.add(ob);

        }
        return new OrientBlob(binaryRecordList);
      }

      return null;
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the BLOB at column '" + columnLabel + "'", e);
    }

  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    return getBoolean(fieldNames.get(getFieldIndex(columnIndex)));
  }

  @SuppressWarnings("boxing")
  public boolean getBoolean(String columnLabel) throws SQLException {
    try {
      return (Boolean) document.field(columnLabel, OType.BOOLEAN);
    } catch (Exception e) {
      throw new SQLException(
          "An error occurred during the retrieval of the boolean value at column '" + columnLabel + "' ---> " + document.toJSON(),
          e);
    }

  }

  @SuppressWarnings("boxing")
  public byte getByte(int columnIndex) throws SQLException {
    return getByte(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public byte getByte(String columnLabel) throws SQLException {
    try {
      return (Byte) document.field(columnLabel, OType.BYTE);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the byte value at column '" + columnLabel + "'", e);
    }
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return getBytes(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    try {

      Object value = document.field(columnLabel);
      if (value == null)
        return null;
      else {
        if (value instanceof OBlob)
          return ((OBlob) value).toStream();
        return document.field(columnLabel, OType.BINARY);
      }
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the bytes value at column '" + columnLabel + "'", e);
    }
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
    return concurrency;
  }

  public String getCursorName() throws SQLException {
    return null;
  }

  public Date getDate(int columnIndex) throws SQLException {
    return getDate(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public Date getDate(final String columnLabel) throws SQLException {
    try {
      activateDatabaseOnCurrentThread();

      java.util.Date date = document.field(columnLabel, OType.DATETIME);
      return date != null ? new Date(date.getTime()) : null;
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the date value at column '" + columnLabel + "'", e);
    }
  }

  public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
    return getDate(fieldNames.get(getFieldIndex(columnIndex)), cal);
  }

  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    if (cal == null)
      throw new SQLException();
    try {
      activateDatabaseOnCurrentThread();

      java.util.Date date = document.field(columnLabel, OType.DATETIME);
      if (date == null)
        return null;

      cal.setTimeInMillis(date.getTime());
      return new Date(cal.getTimeInMillis());
    } catch (Exception e) {
      throw new SQLException(
          "An error occurred during the retrieval of the date value (calendar) " + "at column '" + columnLabel + "'", e);
    }
  }

  public double getDouble(final int columnIndex) throws SQLException {
    int fieldIndex = getFieldIndex(columnIndex);
    return getDouble(fieldNames.get(fieldIndex));
  }

  public double getDouble(final String columnLabel) throws SQLException {
    try {
      final Double r = document.field(columnLabel, OType.DOUBLE);
      return r != null ? r : 0;
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the double value at column '" + columnLabel + "'", e);
    }
  }

  public int getFetchDirection() throws SQLException {
    return 0;
  }

  public void setFetchDirection(int direction) throws SQLException {

  }

  public int getFetchSize() throws SQLException {
    return rowCount;
  }

  public void setFetchSize(int rows) throws SQLException {

  }

  public float getFloat(int columnIndex) throws SQLException {

    return getFloat(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public float getFloat(String columnLabel) throws SQLException {
    try {
      final Float r = document.field(columnLabel, OType.FLOAT);
      return r != null ? r : 0;
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the float value at column '" + columnLabel + "'", e);
    }
  }

  public int getHoldability() throws SQLException {
    return holdability;
  }

  public int getInt(int columnIndex) throws SQLException {
    return getInt(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public int getInt(String columnLabel) throws SQLException {
    if ("@version".equals(columnLabel))
      return document.getVersion();

    try {
      final Integer r = document.field(columnLabel, OType.INTEGER);
      return r != null ? r : 0;

    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the integer value at column '" + columnLabel + "'", e);
    }
  }

  public long getLong(int columnIndex) throws SQLException {
    return getLong(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public long getLong(String columnLabel) throws SQLException {

    try {
      final Long r = document.field(columnLabel, OType.LONG);
      return r != null ? r : 0;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException("An error occurred during the retrieval of the long value at column '" + columnLabel + "'", e);
    }
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
    return getNString(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public String getNString(String columnLabel) throws SQLException {
    try {
      return document.field(columnLabel, OType.STRING);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the string value at column '" + columnLabel + "'", e);
    }
  }

  public Object getObject(int columnIndex) throws SQLException {
    return getObject(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public Object getObject(String columnLabel) throws SQLException {
    if ("@rid".equals(columnLabel) || "rid".equals(columnLabel)) {
      return ((ODocument) document.field("rid")).getIdentity().toString();
    }
    if ("@class".equals(columnLabel) || "class".equals(columnLabel))
      return ((ODocument) document.field("rid")).getClassName();

    try {
      Object value = document.field(columnLabel);
      if (value == null) {
        return null;
      } else {
        // resolve the links so that the returned set contains instances
        // of ODocument
        if (value instanceof ORecordLazyMultiValue) {
          ORecordLazyMultiValue lazyRecord = (ORecordLazyMultiValue) value;
          lazyRecord.convertLinks2Records();
          return lazyRecord;
        } else {
          return value;
        }
      }
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the Java Object at column '" + columnLabel + "'", e);
    }
  }

  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("This method has not been implemented.");
  }

  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("This method has not been implemented.");
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

  public RowId getRowId(final int columnIndex) throws SQLException {
    try {
      return new OrientRowId(document.getIdentity());
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the rowid for record '" + document + "'", e);
    }
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    return getRowId(0);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {

    return null;
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {

    return null;
  }

  public short getShort(int columnIndex) throws SQLException {

    return getShort(fieldNames.get(getFieldIndex(columnIndex)));
  }

  @SuppressWarnings("boxing")
  public short getShort(String columnLabel) throws SQLException {
    try {
      final Short r = document.field(columnLabel, OType.SHORT);
      return r != null ? r : 0;

    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the short value at column '" + columnLabel + "'", e);
    }
  }

  public String getString(int columnIndex) throws SQLException {

    return getString(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public String getString(String columnLabel) throws SQLException {

    if ("@rid".equals(columnLabel) || "rid".equals(columnLabel)) {
      return ((ODocument) document.field("rid")).getIdentity().toString();
    }

    if ("@class".equals(columnLabel) || "class".equals(columnLabel)) {
      if (document.getClassName() != null)
        return document.getClassName();
      return ((ODocument) document.field("rid")).getClassName();
    }

    try {
      return document.field(columnLabel, OType.STRING);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the string value at column '" + columnLabel + "'", e);
    }

  }

  public Time getTime(int columnIndex) throws SQLException {
    return getTime(fieldNames.get(getFieldIndex(columnIndex)));
  }

  public Time getTime(String columnLabel) throws SQLException {
    try {
      java.util.Date date = document.field(columnLabel, OType.DATETIME);
      return getTime(date);
    } catch (Exception e) {
      throw new SQLException("An error occurred during the retrieval of the time value at column '" + columnLabel + "'", e);
    }
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    Date date = getDate(columnIndex, cal);
    return getTime(date);
  }

  private Time getTime(java.util.Date date) {
    return date != null ? new Time(date.getTime()) : null;
  }

  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    Date date = getDate(columnLabel, cal);
    return getTime(date);
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Date date = getDate(columnIndex);
    return getTimestamp(date);
  }

  private Timestamp getTimestamp(Date date) {
    return date != null ? new Timestamp(date.getTime()) : null;
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    Date date = getDate(columnLabel);
    return getTimestamp(date);
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    Date date = getDate(columnIndex, cal);
    return getTimestamp(date);
  }

  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    Date date = getDate(columnLabel, cal);
    return getTimestamp(date);
  }

  public int getType() throws SQLException {
    return type;
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
    return ODocument.class.isAssignableFrom(iface);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(document);
    } catch (ClassCastException e) {
      throw new SQLException(e);
    }
  }

  public void cancelRowUpdates() throws SQLException {
  }

  public void clearWarnings() throws SQLException {
  }

  public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
    return null;
  }

  public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
    return null;
  }
}
