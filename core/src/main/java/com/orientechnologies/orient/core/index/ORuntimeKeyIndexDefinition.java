/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Index definition that use the serializer specified at run-time not based on type. This is useful to have custom type keys for
 * indexes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORuntimeKeyIndexDefinition<T> extends OAbstractIndexDefinition {
  private OBinarySerializer<T> serializer;

  @SuppressWarnings("unchecked")
  public ORuntimeKeyIndexDefinition(final byte iId, int version) {
    super();

    serializer = (OBinarySerializer<T>) OBinarySerializerFactory.getInstance().getObjectSerializer(iId);
    if (serializer == null)
      throw new OConfigurationException("Runtime index definition cannot find binary serializer with id=" + iId
          + ". Assure to plug custom serializer into the server.");
  }

  public ORuntimeKeyIndexDefinition() {
  }

  public List<String> getFields() {
    return Collections.emptyList();
  }

  public List<String> getFieldsToIndex() {
    return Collections.emptyList();
  }

  public String getClassName() {
    return null;
  }

  public Comparable<?> createValue(final List<?> params) {
    return (Comparable<?>) params.get(0);
  }

  public Comparable<?> createValue(final Object... params) {
    return createValue(Arrays.asList(params));
  }

  public int getParamCount() {
    return 1;
  }

  public OType[] getTypes() {
    return null;
  }

  @Override
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      serializeToStream();
      return document;
    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  @Override
  protected void serializeToStream() {
    super.serializeToStream();

    document.field("keySerializerId", serializer.getId());
    document.field("collate", collate.getName());
    document.field("nullValuesIgnored", isNullValuesIgnored());
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void fromStream() {
    serializeFromStream();
  }

  @Override
  protected void serializeFromStream() {
    super.serializeFromStream();

    final byte keySerializerId = ((Number) document.field("keySerializerId")).byteValue();
    serializer = (OBinarySerializer<T>) OBinarySerializerFactory.getInstance().getObjectSerializer(keySerializerId);
    if (serializer == null)
      throw new OConfigurationException("Runtime index definition cannot find binary serializer with id=" + keySerializerId
          + ". Assure to plug custom serializer into the server.");

    String collateField = document.field("collate");
    if (collateField == null)
      collateField = ODefaultCollate.NAME;
    setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean> field("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(final ODocument iDocument) {
    throw new OIndexException("This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final ORuntimeKeyIndexDefinition<?> that = (ORuntimeKeyIndexDefinition<?>) o;
    return serializer.equals(that.serializer);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + serializer.getId();
    return result;
  }

  @Override
  public String toString() {
    return "ORuntimeKeyIndexDefinition{" + "serializer=" + serializer.getId() + '}';
  }

  /**
   * {@inheritDoc}
   * 
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(final String indexName, final String indexType,String engine) {
    final StringBuilder ddl = new StringBuilder("create index ");
    ddl.append(indexName).append(' ').append(indexType).append(' ');
    ddl.append("runtime ").append(serializer.getId());
    return ddl.toString();
  }

  public OBinarySerializer<T> getSerializer() {
    return serializer;
  }

  @Override
  public boolean isAutomatic() {
    return getClassName() != null;
  }
}
