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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index definition for index which is bound to field with type {@link OType#LINKBAG} .
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public class OPropertyRidBagIndexDefinition extends OAbstractIndexDefinitionMultiValue {
  private static final long serialVersionUID = -8315498456603024776L;

  public OPropertyRidBagIndexDefinition() {}

  public OPropertyRidBagIndexDefinition(String className, String field) {
    super(className, field, OType.LINK);
  }

  @Override
  public Object createSingleValue(Object... param) {
    return OType.convert(param[0], keyType.getDefaultJavaType());
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
      default:
        throw new IllegalArgumentException("Invalid change type : " + changeEvent.getChangeType());
    }
  }

  @Override
  public Object getDocumentValueToIndex(ODocument iDocument) {
    return createValue(iDocument.<Object>field(field));
  }

  @Override
  public Object createValue(final List<?> params) {
    if (!(params.get(0) instanceof ORidBag)) return null;

    final ORidBag ridBag = (ORidBag) params.get(0);
    final List<Object> values = new ArrayList<Object>();
    for (final OIdentifiable item : ridBag) {
      values.add(createSingleValue(item));
    }

    return values;
  }

  @Override
  public Object createValue(final Object... params) {
    if (!(params[0] instanceof ORidBag)) return null;

    final ORidBag ridBag = (ORidBag) params[0];
    final List<Object> values = new ArrayList<Object>();
    for (final OIdentifiable item : ridBag) {
      values.add(createSingleValue(item));
    }

    return values;
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType, String engine) {
    return createIndexDDLWithoutFieldType(indexName, indexType, engine).toString();
  }
}
