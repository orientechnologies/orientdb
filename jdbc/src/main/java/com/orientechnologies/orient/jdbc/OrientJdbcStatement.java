/**
 * Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
public class OrientJdbcStatement implements Statement {

  protected final OrientJdbcConnection connection;
  protected final ODatabaseDocument    database;

  // protected OCommandSQL query;
  protected OCommandRequest            query;
  protected List<ODocument>            documents;
  protected boolean                    closed;
  protected Object                     rawResult;
  protected OrientJdbcResultSet        resultSet;
  protected List<String>               batches;

  protected int                        resultSetType;
  protected int                        resultSetConcurrency;
  protected int                        resultSetHoldability;

  public OrientJdbcStatement(final OrientJdbcConnection iConnection) {
    this(iConnection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @throws SQLException
   */
  public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency) throws SQLException {
    this(iConnection, resultSetType, resultSetConcurrency, resultSetType);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @param resultSetHoldability
   */
  public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) {
    this.connection = iConnection;
    this.database = iConnection.getDatabase();
    database.activateOnCurrentThread();
    documents = emptyList();
    batches = new ArrayList<String>();
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
  }

  @Override
  public boolean execute(final String sql) throws SQLException {
    if ("".equals(sql))
      return false;

    if (sql.equalsIgnoreCase("select 1")) {
      documents = new ArrayList<ODocument>(1);
      documents.add(new ODocument().field("1", 1));
    } else {
      query = new OCommandSQL(sql);
      try {
        rawResult = executeCommand(query);
        if (rawResult instanceof List<?>) {
          documents = (List<ODocument>) rawResult;
        } else if (rawResult instanceof OIdentifiable) {
          documents = new ArrayList<ODocument>(1);
          documents.add((ODocument) ((OIdentifiable) rawResult).getRecord());
        } else {
          return false;
        }

      } catch (OQueryParsingException e) {
        throw new SQLSyntaxErrorException("Error while parsing query", e);
      } catch (OException e) {
        throw new SQLException("Error while executing query", e);

      }
    }
    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability);
    return true;

  }

  public ResultSet executeQuery(final String sql) throws SQLException {
    if (execute(sql))
      return resultSet;
    else
      return null;
  }

  public int executeUpdate(final String sql) throws SQLException {
    query = new OCommandSQL(sql);
    rawResult = executeCommand(query);

    if (rawResult instanceof ODocument)
      return 1;
    else if (rawResult instanceof Integer)
      return (Integer) rawResult;
    else if (rawResult instanceof Collection)
      return ((Collection) rawResult).size();

    return 0;
  }

  protected <RET> RET executeCommand(OCommandRequest query) throws SQLException {

    try {
      return database.command(query).execute();
    } catch (OQueryParsingException e) {
      throw new SQLSyntaxErrorException("Error while parsing command", e);
    } catch (OException e) {
      throw new SQLException("Error while executing command", e);

    }
  }

  public int executeUpdate(final String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  public int executeUpdate(final String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  public int executeUpdate(final String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public void close() throws SQLException {
    query = null;
    closed = true;
  }

  public boolean execute(final String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, String[] columnNames) throws SQLException {
    return false;
  }

  public void addBatch(final String sql) throws SQLException {
    batches.add(sql);
  }

  public void cancel() throws SQLException {
  }

  public void clearBatch() throws SQLException {
    batches.clear();
  }

  public void clearWarnings() throws SQLException {
  }

  public int[] executeBatch() throws SQLException {
    int[] results = new int[batches.size()];
    int i = 0;
    for (String sql : batches) {
      results[i++] = executeUpdate(sql);
    }
    return results;
  }

  public int getFetchDirection() throws SQLException {

    return 0;
  }

  public void setFetchDirection(final int direction) throws SQLException {

  }

  public int getFetchSize() throws SQLException {

    return 0;
  }

  public void setFetchSize(final int rows) throws SQLException {

  }

  public ResultSet getGeneratedKeys() throws SQLException {

    return null;
  }

  public int getMaxFieldSize() throws SQLException {

    return 0;
  }

  public void setMaxFieldSize(final int max) throws SQLException {

  }

  public int getMaxRows() throws SQLException {

    return 0;
  }

  public void setMaxRows(final int max) throws SQLException {

  }

  public boolean getMoreResults() throws SQLException {

    return false;
  }

  public boolean getMoreResults(final int current) throws SQLException {

    return false;
  }

  public int getQueryTimeout() throws SQLException {

    return 0;
  }

  public void setQueryTimeout(final int seconds) throws SQLException {

  }

  public ResultSet getResultSet() throws SQLException {

    return resultSet;
  }

  public int getResultSetConcurrency() throws SQLException {

    return resultSet.getConcurrency();
  }

  public int getResultSetHoldability() throws SQLException {

    return resultSet.getHoldability();
  }

  public int getResultSetType() throws SQLException {

    return resultSet.getType();
  }

  public int getUpdateCount() throws SQLException {
    if (isClosed())
      throw new SQLException("Statement already closed");

    return -1;

  }

  public SQLWarning getWarnings() throws SQLException {

    return null;
  }

  public boolean isClosed() throws SQLException {

    return query == null;
  }

  public boolean isPoolable() throws SQLException {

    return false;
  }

  public void setPoolable(final boolean poolable) throws SQLException {

  }

  public void setCursorName(final String name) throws SQLException {

  }

  public void setEscapeProcessing(final boolean enable) throws SQLException {

  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    try {
      // the following if-then structure makes sense if the query can be a
      // subclass of OCommandSQL.

      if (this.query == null) {
        return OCommandSQL.class.isAssignableFrom(iface);
      } else {
        return this.query.getClass().isAssignableFrom(iface);
      }
    } catch (NullPointerException e) {
      throw new SQLException(e);
    }
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(query);
    } catch (ClassCastException e) {
      throw new SQLException(e);
    }
  }

  public void closeOnCompletion() throws SQLException {

  }

  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

}
