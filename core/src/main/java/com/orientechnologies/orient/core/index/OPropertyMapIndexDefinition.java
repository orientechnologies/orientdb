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

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Index implementation bound to one schema class property that presents {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDMAP
 * or
 *
 * @link com.orientechnologies.orient.core.metadata.schema.OType#LINKMAP} property.
 */
public class OPropertyMapIndexDefinition extends OAbstractIndexDefinitionMultiValue {

  private static final long serialVersionUID = 275241019910834466L;

  /** Indicates whether Map will be indexed using its keys or values. */
  public static enum INDEX_BY {
    KEY,
    VALUE
  }

  private INDEX_BY indexBy = INDEX_BY.KEY;

  public OPropertyMapIndexDefinition() {}

  public OPropertyMapIndexDefinition(
      final String iClassName, final String iField, final OType iType, final INDEX_BY indexBy) {
    super(iClassName, iField, iType);

    if (indexBy == null)
      throw new NullPointerException(
          "You have to provide way by which map entries should be mapped");

    this.indexBy = indexBy;
  }

  @Override
  public Object getDocumentValueToIndex(ODocument iDocument) {
    return createValue(iDocument.<Object>field(field));
  }

  @Override
  public Object createValue(List<?> params) {
    if (!(params.get(0) instanceof Map)) return null;

    final Collection<?> mapParams = extractMapParams((Map<?, ?>) params.get(0));
    final List<Object> result = new ArrayList<Object>(mapParams.size());
    for (final Object mapParam : mapParams) {
      result.add(createSingleValue(mapParam));
    }

    return result;
  }

  @Override
  public Object createValue(Object... params) {
    if (!(params[0] instanceof Map)) return null;

    final Collection<?> mapParams = extractMapParams((Map<?, ?>) params[0]);

    final List<Object> result = new ArrayList<>(mapParams.size());
    for (final Object mapParam : mapParams) {
      Object val = createSingleValue(mapParam);
      result.add(val);
    }
    if (getFieldsToIndex().size() == 1 && result.size() == 1) {
      return result.get(0);
    }
    return result;
  }

  public INDEX_BY getIndexBy() {
    return indexBy;
  }

  @Override
  protected void serializeToStream() {
    super.serializeToStream();
    document.field("mapIndexBy", indexBy.toString());
  }

  @Override
  protected void serializeFromStream() {
    super.serializeFromStream();
    indexBy = INDEX_BY.valueOf(document.<String>field("mapIndexBy"));
  }

  private Collection<?> extractMapParams(Map<?, ?> map) {
    if (indexBy == INDEX_BY.KEY) return map.keySet();
    return map.values();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    OPropertyMapIndexDefinition that = (OPropertyMapIndexDefinition) o;

    if (indexBy != that.indexBy) return false;

    return true;
  }

  public Object createSingleValue(final Object... param) {
    return OType.convert(param[0], keyType.getDefaultJavaType());
  }

  public void processChangeEvent(
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    final boolean result;
    if (indexBy.equals(INDEX_BY.KEY))
      result = processKeyChangeEvent(changeEvent, keysToAdd, keysToRemove);
    else result = processValueChangeEvent(changeEvent, keysToAdd, keysToRemove);

    if (!result)
      throw new IllegalArgumentException("Invalid change type :" + changeEvent.getChangeType());
  }

  private boolean processKeyChangeEvent(
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    switch (changeEvent.getChangeType()) {
      case ADD:
        processAdd(createSingleValue(changeEvent.getKey()), keysToAdd, keysToRemove);
        return true;
      case REMOVE:
        processRemoval(createSingleValue(changeEvent.getKey()), keysToAdd, keysToRemove);
        return true;
      case UPDATE:
        return true;
    }
    return false;
  }

  private boolean processValueChangeEvent(
      final OMultiValueChangeEvent<?, ?> changeEvent,
      final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    switch (changeEvent.getChangeType()) {
      case ADD:
        processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
        return true;
      case REMOVE:
        processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
        return true;
      case UPDATE:
        processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
        processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
        return true;
    }
    return false;
  }

  @Override
  public List<String> getFieldsToIndex() {
    if (indexBy == INDEX_BY.KEY) return Collections.singletonList(field + " by key");
    return Collections.singletonList(field + " by value");
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + indexBy.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OPropertyMapIndexDefinition{" + "indexBy=" + indexBy + "} " + super.toString();
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType, String engine) {
    final StringBuilder ddl = new StringBuilder("create index `");

    ddl.append(indexName).append("` on `");
    ddl.append(className).append("` ( `").append(field).append("`");

    if (indexBy == INDEX_BY.KEY) ddl.append(" by key");
    else ddl.append(" by value");

    ddl.append(" ) ");
    ddl.append(indexType);

    return ddl.toString();
  }
}
