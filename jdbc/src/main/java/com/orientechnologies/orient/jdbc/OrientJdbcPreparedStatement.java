/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://orientdb.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.jdbc.OrientJdbcParameterMetadata.ParameterDefinition;
import java.io.IOException;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OrientJdbcPreparedStatement extends OrientJdbcStatement implements PreparedStatement {

  protected final Map<Integer, Object> params;

  public OrientJdbcPreparedStatement(OrientJdbcConnection iConnection, String sql) {
    this(
        iConnection,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT,
        sql);
  }

  public OrientJdbcPreparedStatement(
      OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency, String sql)
      throws SQLException {
    this(iConnection, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT, sql);
  }

  public OrientJdbcPreparedStatement(
      OrientJdbcConnection iConnection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability,
      String sql) {
    super(iConnection, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.sql = sql;
    params = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  public ResultSet executeQuery() throws SQLException {

    //    return super.executeQuery(sql);
    sql = mayCleanForSpark(sql);

    if (sql.equalsIgnoreCase("select 1")) {
      // OPTIMIZATION
      OResultInternal element = new OResultInternal();
      element.setProperty("1", 1);
      OInternalResultSet rs = new OInternalResultSet();
      rs.add(element);
      oResultSet = rs;
    } else {
      try {
        //        sql = new OSQLSynchQuery<ODocument>(mayCleanForSpark(sql));
        oResultSet = database.query(sql, params.values().toArray());

      } catch (OQueryParsingException e) {
        throw new SQLSyntaxErrorException("Error while parsing query", e);
      } catch (OException e) {
        throw new SQLException("Error while executing query", e);
      }
    }

    // return super.executeQuery(sql);
    resultSet =
        new OrientJdbcResultSet(
            this, oResultSet, resultSetType, resultSetConcurrency, resultSetHoldability);
    return resultSet;
  }

  public int executeUpdate() throws SQLException {
    return this.executeUpdate(sql);
  }

  @Override
  protected OResultSet executeCommand(String query) throws SQLException {

    try {
      database.activateOnCurrentThread();
      return database.command(query, params.values().toArray());
    } catch (OException e) {
      throw new SQLException("Error while executing command", e);
    }
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    params.put(parameterIndex, null);
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void clearParameters() throws SQLException {
    params.clear();
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    params.put(parameterIndex, x);
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    params.put(parameterIndex, x);
  }

  public boolean execute() throws SQLException {
    return this.execute(sql);
  }

  public void addBatch() throws SQLException {

    //    batches.add(sql);
    throw new UnsupportedOperationException();
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    if (resultSet == null) executeQuery();

    return getResultSet().getMetaData();
  }

  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    params.put(parameterIndex, new java.util.Date(x.getTime()));
  }

  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    params.put(parameterIndex, new java.util.Date(x.getTime()));
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    params.put(parameterIndex, new java.util.Date(x.getTime()));
  }

  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    params.put(parameterIndex, null);
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    params.put(parameterIndex, null);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {

    OrientJdbcParameterMetadata parameterMetadata = new OrientJdbcParameterMetadata();
    int start = 0;
    int index = sql.indexOf('?', start);
    while (index > 0) {
      final ParameterDefinition def = new ParameterDefinition();
      // TODO find a way to know a bit more on each parameter

      parameterMetadata.add(def);
      start = index + 1;
      index = sql.indexOf('?', start);
    }

    return parameterMetadata;
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    params.put(parameterIndex, ((OrientRowId) x).rid);
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    params.put(parameterIndex, value);
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    setBinaryStream(parameterIndex, x);
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    try {
      ORecordBytes record = new ORecordBytes();
      record.fromInputStream(x);
      record.save();
      params.put(parameterIndex, record);
    } catch (IOException e) {
      throw new SQLException("unable to store inputStream", e);
    }
  }

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
