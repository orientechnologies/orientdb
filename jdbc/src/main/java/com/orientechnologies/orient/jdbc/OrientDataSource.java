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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class OrientDataSource implements DataSource {

  static {
    try {
      Class.forName(OrientJdbcDriver.class.getCanonicalName());
    } catch (ClassNotFoundException e) {
      System.err.println("OrientDB DataSource unable to load OrientDB JDBC Driver");
    }
  }

  private String      url;
  private String      username;
  private String      password;

  private PrintWriter logger;
  private int         loginTimeout;

  public PrintWriter getLogWriter() throws SQLException {
    return logger;
  }

  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logger = out;

  }

  public void setLoginTimeout(int seconds) throws SQLException {
    this.loginTimeout = seconds;

  }

  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public Connection getConnection() throws SQLException {
    return this.getConnection(username, password);
  }

  public Connection getConnection(String username, String password) throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {

    throw new SQLFeatureNotSupportedException();
  }

}
