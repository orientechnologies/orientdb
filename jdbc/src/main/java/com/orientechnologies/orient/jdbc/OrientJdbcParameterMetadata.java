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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Johann Sorel (Geomatys)
 */
class OrientJdbcParameterMetadata implements ParameterMetaData {

  static class ParameterDefinition {
    public int     nullable  = parameterNullableUnknown;
    public boolean signed    = true;
    public int     precision = 0;
    public int     scale     = 0;
    public int     type      = java.sql.Types.OTHER;
    public String  typeName  = "String";
    public String  className = "java.lang.String";
    public int     mode      = parameterModeUnknown;

  }

  private final List<ParameterDefinition> definitions;

  public OrientJdbcParameterMetadata() {
    this.definitions = new ArrayList<ParameterDefinition>();
  }

  @Override
  public int getParameterCount() throws SQLException {
    return definitions.size();
  }

  @Override
  public int isNullable(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).nullable;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).signed;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).precision;
  }

  @Override
  public int getScale(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).scale;
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).type;
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).typeName;
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).className;
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    checkIndex(param);
    return definitions.get(param).mode;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("No object wrapper for class : " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  private void checkIndex(int index) throws SQLException {
    if (index < 0 || index >= definitions.size()) {
      throw new SQLException("Parameter number " + index + " does not exist.");
    }
  }

  public boolean add(ParameterDefinition parameterDefinition) {
    return definitions.add(parameterDefinition);
  }
}
