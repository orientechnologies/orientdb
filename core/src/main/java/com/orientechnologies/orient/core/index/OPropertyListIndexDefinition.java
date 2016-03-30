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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;

/**
 * Index implementation bound to one schema class property that presents
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDLIST},
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#LINKLIST},
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#LINKSET} or
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDSET} properties.
 */
public class OPropertyListIndexDefinition extends OAbstractIndexDefinitionMultiValue implements OIndexDefinitionMultiValue {

  public OPropertyListIndexDefinition(final String iClassName, final String iField, final OType iType) {
    super(iClassName, iField, iType);
  }

  public OPropertyListIndexDefinition() {
  }

  @Override
  public Object getDocumentValueToIndex(ODocument iDocument) {
    return createValue(iDocument.field(field));
  }

  @Override
  public Object createValue(List<?> params) {
    if (!(params.get(0) instanceof Collection))
      params = (List) Collections.singletonList(params);

    final Collection<?> multiValueCollection = (Collection<?>) params.get(0);
    final List<Object> values = new ArrayList<Object>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(item));
    }
    return values;
  }

  @Override
  public Object createValue(final Object... params) {
    if (!(params[0] instanceof Collection)) {
      return null;
    }

    final Collection<?> multiValueCollection = (Collection<?>) params[0];
    final List<Object> values = new ArrayList<Object>(multiValueCollection.size());
    for (final Object item : multiValueCollection) {
      values.add(createSingleValue(item));
    }
    return values;
  }

  public Object createSingleValue(final Object... param) {
    try {
      return OType.convert(param[0], keyType.getDefaultJavaType());
    } catch (Exception e) {
      throw new OIndexException("Invalid key for index: " + param[0] + " cannot be converted to " + keyType, e);
    }
  }

  public void processChangeEvent(final OMultiValueChangeEvent<?, ?> changeEvent, final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    switch (changeEvent.getChangeType()) {
    case ADD: {
      processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
      break;
    }
    case REMOVE: {
      processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
      break;
    }
    case UPDATE: {
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
