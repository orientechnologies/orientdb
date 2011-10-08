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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;

public class OrientJdbcMetaData implements ResultSetMetaData {
	
	private final  static Map<OType, Integer> oTypesSqlTypes = new HashMap<OType, Integer>();
	
	static {
		oTypesSqlTypes.put(OType.STRING, Types.VARCHAR);
		oTypesSqlTypes.put(OType.INTEGER, Types.INTEGER);
		oTypesSqlTypes.put(OType.FLOAT, Types.FLOAT);
		oTypesSqlTypes.put(OType.SHORT, Types.SMALLINT);
		oTypesSqlTypes.put(OType.BOOLEAN, Types.BOOLEAN);
		oTypesSqlTypes.put(OType.LONG, Types.BIGINT);
		oTypesSqlTypes.put(OType.DOUBLE, Types.DECIMAL);
	}
	
	private OrientJdbcResultSet resultSet;

	public OrientJdbcMetaData(OrientJdbcResultSet iResultSet) {
		resultSet = iResultSet;
	}

	
	public int getColumnCount() throws SQLException {
		return 0;
	}

	public String getCatalogName(int column) throws SQLException {

		return null;
	}

	public String getColumnClassName(int column) throws SQLException {

		return null;
	}

	public int getColumnDisplaySize(int column) throws SQLException {

		return 0;
	}

	public String getColumnLabel(int column) throws SQLException {

		return null;
	}

	public String getColumnName(int column) throws SQLException {

		return null;
	}

	public int getColumnType(int column) throws SQLException {

		return 0;
	}

	public String getColumnTypeName(int column) throws SQLException {

		return null;
	}

	public int getPrecision(int column) throws SQLException {

		return 0;
	}

	public int getScale(int column) throws SQLException {

		return 0;
	}

	public String getSchemaName(int column) throws SQLException {

		return null;
	}

	public String getTableName(int column) throws SQLException {

		return null;
	}

	public boolean isAutoIncrement(int column) throws SQLException {

		return false;
	}

	public boolean isCaseSensitive(int column) throws SQLException {

		return false;
	}

	public boolean isCurrency(int column) throws SQLException {

		return false;
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {

		return false;
	}

	public int isNullable(int column) throws SQLException {

		return 0;
	}

	public boolean isReadOnly(int column) throws SQLException {

		return false;
	}

	public boolean isSearchable(int column) throws SQLException {

		return false;
	}

	public boolean isSigned(int column) throws SQLException {

		return false;
	}

	public boolean isWritable(int column) throws SQLException {

		return false;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {

		return false;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {

		return null;
	}

}
