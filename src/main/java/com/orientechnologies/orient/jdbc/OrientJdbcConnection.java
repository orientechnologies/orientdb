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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;

/**
 * 
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
public class OrientJdbcConnection implements Connection {

  private final String     dbUrl;
  private final Properties info;
  private OGraphDatabase   database;
  private boolean          readOnly = false;
  private boolean          autoCommit;

  public OrientJdbcConnection(String iUrl, Properties iInfo) {
    dbUrl = iUrl.replace("jdbc:orient:", "");

    info = iInfo;

    String username = iInfo.getProperty("user", "admin");
    String password = iInfo.getProperty("password", "admin");

    database = OGraphDatabasePool.global().acquire(dbUrl, username, password);

  }

  public void clearWarnings() throws SQLException {
  }

  public void close() throws SQLException {
    database.close();
  }

  public void commit() throws SQLException {
    database.commit();
  }

  public void rollback() throws SQLException {
    database.rollback();
  }

  public boolean isClosed() throws SQLException {
    return database.isClosed();
  }

  public boolean isReadOnly() throws SQLException {
    return readOnly;
  }

  public void setReadOnly(boolean iReadOnly) throws SQLException {
    readOnly = iReadOnly;
  }

  public boolean isValid(int timeout) throws SQLException {
    return true;
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {

    return null;
  }

  public Blob createBlob() throws SQLException {

    return null;
  }

  public Clob createClob() throws SQLException {

    return null;
  }

  public NClob createNClob() throws SQLException {

    return null;
  }

  public SQLXML createSQLXML() throws SQLException {

    return null;
  }

  public Statement createStatement() throws SQLException {
    return new OrientJdbcStatement(this);
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return new OrientJdbcStatement(this);
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return new OrientJdbcStatement(this);
  }

  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {

    return null;
  }

  public boolean getAutoCommit() throws SQLException {

    return autoCommit;
  }

  public String getCatalog() throws SQLException {
    return database.getName();
  }

  public Properties getClientInfo() throws SQLException {

    return null;
  }

  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return new OrientJdbcDatabaseMetaData(this, getDatabase());
  }

  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_SERIALIZABLE;
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new OrientJdbcPreparedStatement(this, sql);
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.autoCommit = autoCommit;
  }

  public void setCatalog(String catalog) throws SQLException {

  }

  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // noop
  }

  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // noop
  }

  public void setHoldability(int holdability) throws SQLException {

  }

  public Savepoint setSavepoint() throws SQLException {

    return null;
  }

  public Savepoint setSavepoint(String name) throws SQLException {

    return null;
  }

  public void setTransactionIsolation(int level) throws SQLException {

  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {

    throw new SQLFeatureNotSupportedException();
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {

    throw new SQLFeatureNotSupportedException();
  }

  public String getUrl() {
    return dbUrl;
  }

  public OGraphDatabase getDatabase() {
    return database;
  }

  public void abort(Executor arg0) throws SQLException {

  }

  public int getNetworkTimeout() throws SQLException {
    return OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT.getValueAsInteger();
  }

  /**
   * No schema is supported.
   */
  public String getSchema() throws SQLException {
    return null;
  }

  public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
    OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(arg1);
  }

  public void setSchema(String arg0) throws SQLException {
  }
}
