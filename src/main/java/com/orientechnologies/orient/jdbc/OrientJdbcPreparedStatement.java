/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2011 TXT e-solutions SpA
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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * 
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
public class OrientJdbcPreparedStatement extends OrientJdbcStatement implements PreparedStatement {

  private final String               sql;
  private final Map<Integer, String> params;

  public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql) {
    super(iConnection);
    this.sql = sql;
    params = new HashMap<Integer, String>();
  }

  public ResultSet executeQuery() throws SQLException {
    if (sql.equalsIgnoreCase("select 1")) {
      documents = new ArrayList<ODocument>();
      documents.add(new ODocument().field("1", 1));
    } else {
      query = new OCommandSQL(sql);
      try {
        documents = database.query(new OSQLSynchQuery<ODocument>(sql), params.values().toArray(new Object[] {}));
      } catch (OQueryParsingException e) {
        throw new SQLSyntaxErrorException("Error on parsing the query", e);
      }
    }

    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability);
    return resultSet;
  }

  public int executeUpdate() throws SQLException {
    return 0;
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {

  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    params.put(parameterIndex, Boolean.toString(x));

  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    params.put(parameterIndex, Byte.toString(x));

  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    params.put(parameterIndex, Short.toString(x));
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    params.put(parameterIndex, Integer.toString(x));
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    params.put(parameterIndex, Long.toString(x));
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    params.put(parameterIndex, Float.toString(x));
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    params.put(parameterIndex, Double.toString(x));
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    params.put(parameterIndex, x.toPlainString());
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    params.put(parameterIndex, x);
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
    params.clear();
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

    final List<OrientJdbcParameterMetadata.ParameterDefinition> definitions = new ArrayList<OrientJdbcParameterMetadata.ParameterDefinition>();

    int start = 0;
    int index = sql.indexOf('?', start);
    while (index > 0) {
      final OrientJdbcParameterMetadata.ParameterDefinition def = new OrientJdbcParameterMetadata.ParameterDefinition();
      // TODO find a way to know a bit more on each parameter
      definitions.add(def);
      start = index + 1;
      index = sql.indexOf('?', start);
    }

    return new OrientJdbcParameterMetadata(definitions);
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
