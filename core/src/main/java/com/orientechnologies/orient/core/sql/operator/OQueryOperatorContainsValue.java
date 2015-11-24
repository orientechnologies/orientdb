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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * CONTAINS KEY operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorContainsValue extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContainsValue() {
    super("CONTAINSVALUE", 5, false);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (!(iRight instanceof OSQLFilterCondition) && !(iLeft instanceof OSQLFilterCondition))
      return OIndexReuseType.INDEX_METHOD;

    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    OIndexCursor cursor;
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      if (!((indexDefinition instanceof OPropertyMapIndexDefinition) && ((OPropertyMapIndexDefinition) indexDefinition)
          .getIndexBy() == OPropertyMapIndexDefinition.INDEX_BY.VALUE))
        return null;

      final Object key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));

      if (key == null)
        return null;

      final Object indexResult = index.get(key);
      if (indexResult == null || indexResult instanceof OIdentifiable)
        cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, key);
      else
        cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), key);
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.
      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      if (!((compositeIndexDefinition.getMultiValueDefinition() instanceof OPropertyMapIndexDefinition) && ((OPropertyMapIndexDefinition) compositeIndexDefinition
          .getMultiValueDefinition()).getIndexBy() == OPropertyMapIndexDefinition.INDEX_BY.VALUE))
        return null;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams);

      if (keyOne == null)
        return null;

      if (internalIndex.hasRangeQuerySupport()) {
        final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams);

        cursor = index.iterateEntriesBetween(keyOne, true, keyTwo, true, ascSortOrder);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          final Object indexResult = index.get(keyOne);
          if (indexResult == null || indexResult instanceof OIdentifiable)
            cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, keyOne);
          else
            cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), keyOne);
        } else
          return null;
      }

    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return cursor;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    final OSQLFilterCondition condition;
    if (iCondition.getLeft() instanceof OSQLFilterCondition)
      condition = (OSQLFilterCondition) iCondition.getLeft();
    else if (iCondition.getRight() instanceof OSQLFilterCondition)
      condition = (OSQLFilterCondition) iCondition.getRight();
    else
      condition = null;

    OType type = null;
    if (iCondition.getLeft() instanceof OSQLFilterItemField && ((OSQLFilterItemField) iCondition.getLeft()).isFieldChain()
        && ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemCount() == 1) {
      String fieldName = ((OSQLFilterItemField) iCondition.getLeft()).getFieldChain().getItemName(0);
      if (fieldName != null) {
        Object record = iRecord.getRecord();
        if (record instanceof ODocument) {
          OProperty property = ((ODocument) record).getSchemaClass().getProperty(fieldName);
          if (property != null && property.getType().isMultiValue()) {
            type = property.getLinkedType();
          }
        }
      }
    }

    Object right = iRight;
    if (type != null) {
      right = OType.convert(iRight, type.getDefaultJavaType());
    }

    if (iLeft instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext))
            return true;
        }
      } else
        return map.containsValue(right);

    } else if (iRight instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iRight;

      if (condition != null)
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ODocument) o, null, iContext))
            return true;
          else
            return map.containsValue(iLeft);
        }
    }
    return false;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Object loadIfNeed(Object o) {
    final ORecord record = (ORecord) o;
    if (record.getRecord().getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        o = record.<ORecord> load();
      } catch (ORecordNotFoundException e) {
        throw new OException("Error during loading record with id : " + record.getIdentity(), e);
      }
    }
    return o;
  }
}
