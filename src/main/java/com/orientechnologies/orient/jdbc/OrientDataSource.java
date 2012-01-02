package com.orientechnologies.orient.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

public class OrientDataSource implements DataSource {

	static {
		try {
			Class.forName(OrientJdbcDriver.class.getCanonicalName());
		} catch (ClassNotFoundException e) {
			System.err.println("OrientDB DataSource unable to load OrientDB JDBC Driver");
		}
	}

	private String url;
	private String username;
	private String password;

	private PrintWriter logger;
	private int loginTimeout;

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

}
