/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import java.util.Collections;
import java.util.List;

/** Index implementation bound to one schema class property. */
public class OPropertyIndexDefinition extends OAbstractIndexDefinition {
  private static final long serialVersionUID = 7395728581151922197L;
  protected String className;
  protected String field;
  protected OType keyType;

  public OPropertyIndexDefinition(final String iClassName, final String iField, final OType iType) {
    super();
    className = iClassName;
    field = iField;
    keyType = iType;
  }

  /** Constructor used for index unmarshalling. */
  public OPropertyIndexDefinition() {}

  public String getClassName() {
    return className;
  }

  public List<String> getFields() {
    return Collections.singletonList(field);
  }

  public List<String> getFieldsToIndex() {
    if (collate == null || collate.getName().equals(ODefaultCollate.NAME))
      return Collections.singletonList(field);

    return Collections.singletonList(field + " collate " + collate.getName());
  }

  public Object getDocumentValueToIndex(final ODocument iDocument) {
    if (OType.LINK.equals(keyType)) {
      final OIdentifiable identifiable = iDocument.field(field);
      if (identifiable != null) return createValue(identifiable.getIdentity());
      else return null;
    }
    return createValue(iDocument.<Object>field(field));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    if (!super.equals(o)) return false;

    final OPropertyIndexDefinition that = (OPropertyIndexDefinition) o;

    if (!className.equals(that.className)) return false;
    if (!field.equals(that.field)) return false;
    if (keyType != that.keyType) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + className.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + keyType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OPropertyIndexDefinition{"
        + "className='"
        + className
        + '\''
        + ", field='"
        + field
        + '\''
        + ", keyType="
        + keyType
        + ", collate="
        + collate
        + ", null values ignored = "
        + isNullValuesIgnored()
        + '}';
  }

  public Object createValue(final List<?> params) {
    return OType.convert(params.get(0), keyType.getDefaultJavaType());
  }

  /** {@inheritDoc} */
  public Object createValue(final Object... params) {
    return OType.convert(params[0], keyType.getDefaultJavaType());
  }

  public int getParamCount() {
    return 1;
  }

  public OType[] getTypes() {
    return new OType[] {keyType};
  }

  public void fromStream(ODocument document) {
    this.document = document;
    serializeFromStream();
  }

  @Override
  public final ODocument toStream() {
    serializeToStream();
    return document;
  }

  protected void serializeToStream() {
    super.serializeToStream();

    document.field("className", className);
    document.field("field", field);
    document.field("keyType", keyType.toString());
    document.field("collate", collate.getName());
    document.field("nullValuesIgnored", isNullValuesIgnored());
  }

  protected void serializeFromStream() {
    super.serializeFromStream();

    className = document.field("className");
    field = document.field("field");

    final String keyTypeStr = document.field("keyType");
    keyType = OType.valueOf(keyTypeStr);

    setCollate((String) document.field("collate"));
    setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean>field("nullValuesIgnored")));
  }

  /**
   * {@inheritDoc}
   *
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(
      final String indexName, final String indexType, final String engine) {
    return createIndexDDLWithFieldType(indexName, indexType, engine).toString();
  }

  protected StringBuilder createIndexDDLWithFieldType(
      String indexName, String indexType, String engine) {
    final StringBuilder ddl = createIndexDDLWithoutFieldType(indexName, indexType, engine);
    ddl.append(' ').append(keyType.name());
    return ddl;
  }

  protected StringBuilder createIndexDDLWithoutFieldType(
      final String indexName, final String indexType, final String engine) {
    final StringBuilder ddl = new StringBuilder("create index `");

    ddl.append(indexName).append("` on `");
    ddl.append(className).append("` ( `").append(field).append("`");

    if (!collate.getName().equals(ODefaultCollate.NAME))
      ddl.append(" collate ").append(collate.getName());

    ddl.append(" ) ");
    ddl.append(indexType);

    if (engine != null)
      ddl.append(' ').append(OCommandExecutorSQLCreateIndex.KEYWORD_ENGINE + " " + engine);
    return ddl;
  }

  @Override
  public boolean isAutomatic() {
    return true;
  }
}
