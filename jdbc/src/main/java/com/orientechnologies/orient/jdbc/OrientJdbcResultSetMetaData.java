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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
@SuppressWarnings("boxing")
public class OrientJdbcResultSetMetaData implements ResultSetMetaData {

  private final static Map<OType, Integer> typesSqlTypes = new HashMap<OType, Integer>();

  static {
    typesSqlTypes.put(OType.STRING, Types.VARCHAR);
    typesSqlTypes.put(OType.INTEGER, Types.INTEGER);
    typesSqlTypes.put(OType.FLOAT, Types.FLOAT);
    typesSqlTypes.put(OType.SHORT, Types.SMALLINT);
    typesSqlTypes.put(OType.BOOLEAN, Types.BOOLEAN);
    typesSqlTypes.put(OType.LONG, Types.BIGINT);
    typesSqlTypes.put(OType.DOUBLE, Types.DOUBLE);
    typesSqlTypes.put(OType.DECIMAL, Types.DECIMAL);
    typesSqlTypes.put(OType.DATE, Types.DATE);
    typesSqlTypes.put(OType.DATETIME, Types.TIMESTAMP);
    typesSqlTypes.put(OType.BYTE, Types.TINYINT);
    typesSqlTypes.put(OType.SHORT, Types.SMALLINT);

    // NOT SURE ABOUT THE FOLLOWING MAPPINGS
    typesSqlTypes.put(OType.BINARY, Types.BINARY);
    typesSqlTypes.put(OType.EMBEDDED, Types.JAVA_OBJECT);
    typesSqlTypes.put(OType.EMBEDDEDLIST, Types.ARRAY);
    typesSqlTypes.put(OType.EMBEDDEDMAP, Types.JAVA_OBJECT);
    typesSqlTypes.put(OType.EMBEDDEDSET, Types.ARRAY);
    typesSqlTypes.put(OType.LINK, Types.JAVA_OBJECT);
    typesSqlTypes.put(OType.LINKLIST, Types.ARRAY);
    typesSqlTypes.put(OType.LINKMAP, Types.JAVA_OBJECT);
    typesSqlTypes.put(OType.LINKSET, Types.ARRAY);
    typesSqlTypes.put(OType.TRANSIENT, Types.NULL);
  }

  private OrientJdbcResultSet resultSet;

  public OrientJdbcResultSetMetaData(final OrientJdbcResultSet iResultSet) {
    resultSet = iResultSet;
  }

  public static Integer getSqlType(final OType iType) {
    return typesSqlTypes.get(iType);
  }

  public int getColumnCount() throws SQLException {
    final ODocument currentRecord = getCurrentRecord();
    return currentRecord.fields();
  }

  public String getCatalogName(final int column) throws SQLException {
    // return an empty String according to the method's documentation
    return "";
  }

  public String getColumnClassName(final int column) throws SQLException {
    Object value = this.resultSet.getObject(column);
    if (value == null)
      return null;
    return value.getClass().getCanonicalName();
  }

  public int getColumnDisplaySize(final int column) throws SQLException {
    return 0;
  }

  public String getColumnLabel(final int column) throws SQLException {
    return this.getColumnName(column);
  }

  public String getColumnName(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();
    return currentRecord.fieldNames()[column - 1];
  }

  public int getColumnType(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();

    final String[] fieldNames = currentRecord.fieldNames();

    if (column > fieldNames.length)
      return Types.NULL;

    String fieldName = fieldNames[column - 1];
    // The OClass is not available so attempting to retrieve the OType from
    // the schema class
    // results in a NullPointerException
    // OClass oclass = currentRecord.getSchemaClass();
    OType otype = currentRecord.fieldType(fieldName);

    if (otype == null) {
      Object value = currentRecord.field(fieldName);

      if (value == null) {
        return Types.NULL;
      } else if (value instanceof OBlob) {
        // Check if the type is a binary record or a collection of binary
        // records
        return Types.BINARY;
      } else if (value instanceof ORecordLazyList) {
        ORecordLazyList list = (ORecordLazyList) value;
        // check if all the list items are instances of ORecordBytes
        ListIterator<OIdentifiable> iterator = list.listIterator();
        OIdentifiable listElement;
        boolean stop = false;
        while (iterator.hasNext() && !stop) {
          listElement = iterator.next();
          if (!(listElement instanceof OBlob))
            stop = true;
        }
        if (!stop) {
          return Types.BLOB;
        }
      }
      return this.getSQLTypeFromJavaClass(value);
    } else {
      if (otype == OType.EMBEDDED || otype == OType.LINK) {
        Object value = currentRecord.field(fieldName);
        if (value == null) {
          return Types.NULL;
        }
        // 1. Check if the type is another record or a collection of records
        if (value instanceof OBlob) {
          return Types.BINARY;
        } else {
          // the default type
          return typesSqlTypes.get(otype);
        }
      } else {
        if (otype == OType.EMBEDDEDLIST || otype == OType.LINKLIST) {
          Object value = currentRecord.field(fieldName);
          if (value == null) {
            return Types.NULL;
          }
          if (value instanceof ORecordLazyList) {
            ORecordLazyList list = (ORecordLazyList) value;
            // check if all the list items are instances of ORecordBytes
            ListIterator<OIdentifiable> iterator = list.listIterator();
            OIdentifiable listElement;
            boolean stop = false;
            while (iterator.hasNext() && !stop) {
              listElement = iterator.next();
              if (!(listElement instanceof OBlob))
                stop = true;
            }
            if (stop) {
              return typesSqlTypes.get(otype);
            } else {
              return Types.BLOB;
            }
          } else {
            return typesSqlTypes.get(otype);
//            return Types.JAVA_OBJECT;
          }
        } else {
          return typesSqlTypes.get(otype);
        }
      }

    }
  }

  protected ODocument getCurrentRecord() throws SQLException {
    final ODocument currentRecord = this.resultSet.unwrap(ODocument.class);
    if (currentRecord == null)
      throw new SQLException("No current record");
    return currentRecord;
  }

  private int getSQLTypeFromJavaClass(final Object value) {
    if (value instanceof Boolean)
      return typesSqlTypes.get(OType.BOOLEAN);
    else if (value instanceof Byte)
      return typesSqlTypes.get(OType.BYTE);
    else if (value instanceof Date)
      return typesSqlTypes.get(OType.DATETIME);
    else if (value instanceof Double)
      return typesSqlTypes.get(OType.DOUBLE);
    else if (value instanceof BigDecimal)
      return typesSqlTypes.get(OType.DECIMAL);
    else if (value instanceof Float)
      return typesSqlTypes.get(OType.FLOAT);
    else if (value instanceof Integer)
      return typesSqlTypes.get(OType.INTEGER);
    else if (value instanceof Long)
      return typesSqlTypes.get(OType.LONG);
    else if (value instanceof Short)
      return typesSqlTypes.get(OType.SHORT);
    else if (value instanceof String)
      return typesSqlTypes.get(OType.STRING);
    else if (value instanceof List)
      return typesSqlTypes.get(OType.EMBEDDEDLIST);
    else
      return Types.JAVA_OBJECT;
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();

    OType columnType = currentRecord.fieldType(currentRecord.fieldNames()[column - 1]);
    if (columnType == null)
      return null;
    return columnType.toString();
  }

  public int getPrecision(final int column) throws SQLException {
    return 0;
  }

  public int getScale(final int column) throws SQLException {
    return 0;
  }

  public String getSchemaName(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();
    if (currentRecord == null)
      return "";
    else
      return currentRecord.getDatabase().getName();
  }

  public String getTableName(final int column) throws SQLException {
    final OProperty p = getProperty(column);
    return p != null ? p.getOwnerClass().getName() : null;
  }

  public boolean isAutoIncrement(final int column) throws SQLException {
    return false;
  }

  public boolean isCaseSensitive(final int column) throws SQLException {
    final OProperty p = getProperty(column);
    return p != null ? p.getCollate().getName().equalsIgnoreCase("ci") : false;
  }

  public boolean isCurrency(final int column) throws SQLException {

    return false;
  }

  public boolean isDefinitelyWritable(final int column) throws SQLException {

    return false;
  }

  public int isNullable(final int column) throws SQLException {
    return columnNullableUnknown;
  }

  public boolean isReadOnly(final int column) throws SQLException {
    final OProperty p = getProperty(column);
    return p != null ? p.isReadonly() : false;
  }

  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  public boolean isSigned(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();
    return this.isANumericColumn(currentRecord.fieldType(currentRecord.fieldNames()[column - 1]));
  }

  public boolean isWritable(final int column) throws SQLException {
    return !isReadOnly(column);
  }

  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }

  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return null;
  }

  private boolean isANumericColumn(final OType type) {
    return type == OType.BYTE || type == OType.DOUBLE || type == OType.FLOAT || type == OType.INTEGER || type == OType.LONG
        || type == OType.SHORT;
  }

  protected OProperty getProperty(final int column) throws SQLException {
    final ODocument currentRecord = getCurrentRecord();

    final OClass schemaClass = currentRecord.getSchemaClass();
    if (schemaClass != null) {
      final String fieldName = currentRecord.fieldNames()[column - 1];
      return schemaClass.getProperty(fieldName);
    }

    return null;
  }

}
