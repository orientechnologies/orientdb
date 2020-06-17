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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Index implementation bound to one schema class property that presents {@link
 * com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDLIST}, {@link
 * com.orientechnologies.orient.core.metadata.schema.OType#LINKLIST}, {@link
 * com.orientechnologies.orient.core.metadata.schema.OType#LINKSET} or {@link
 * com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDSET} properties.
 */
public class OPropertyListIndexDefinition extends OAbstractIndexDefinitionMultiValue {

  private static final long serialVersionUID = -6499782365051906190L;

  public OPropertyListIndexDefinition(
      final String iClassName, final String iField, final OType iType) {
    super(iClassName, iField, iType);
  }

  public OPropertyListIndexDefinition() {}

  @Override
  public Object getDocumentValueToIndex(ODocument iDocument) {
    return createValue(iDocument.<Object>field(field));
  }

  @Override
  public Object createValue(List<?> params) {
    if (!(params.get(0) instanceof Collection)) params = (List) Collections.singletonList(params);

    final Collection<?> multiValueCollection = (Collection<?>) params.get(0);
    final List<Object> values = new ArrayList<>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(item));
    }
    return values;
  }

  @Override
  public Object createValue(final Object... params) {
    Object param = params[0];
    if (!(param instanceof Collection)) {
      try {
        return OType.convert(param, keyType.getDefaultJavaType());
      } catch (Exception e) {
        return null;
      }
    }

    final Collection<?> multiValueCollection = (Collection<?>) param;
    final List<Object> values = new ArrayList<>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(item));
    }
    return values;
  }

  public Object createSingleValue(final Object... param) {
    try {
      return OType.convert(param[0], keyType.getDefaultJavaType());
    } catch (Exception e) {
      OException ex =
          OException.wrapException(
              new OIndexException(
                  "Invalid key for index: " + param[0] + " cannot be converted to " + keyType),
              e);
      throw ex;
    }
  }

  public void processChangeEvent(
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    switch (changeEvent.getChangeType()) {
      case ADD:
        {
          processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
          break;
        }
      case REMOVE:
        {
          processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
          break;
        }
      case UPDATE:
        {
          processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
          processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
          break;
        }
      default:
        throw new IllegalArgumentException("Invalid change type : " + changeEvent.getChangeType());
    }
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType, String engine) {
    return createIndexDDLWithoutFieldType(indexName, indexType, engine).toString();
  }
}
