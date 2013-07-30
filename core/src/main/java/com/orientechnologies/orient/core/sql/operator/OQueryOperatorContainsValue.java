/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql.operator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

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

    if (iLeft instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iLeft;

      if (condition != null) {
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ORecordSchemaAware<?>) o, null, iContext))
            return true;
        }
      } else
        return map.containsValue(iRight);

    } else if (iRight instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iRight;

      if (condition != null)
        // CHECK AGAINST A CONDITION
        for (Object o : map.values()) {
          o = loadIfNeed(o);
          if ((Boolean) condition.evaluate((ORecordSchemaAware<?>) o, null, iContext))
            return true;
          else
            return map.containsValue(iLeft);
        }
    }
    return false;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Object loadIfNeed(Object o) {
    final ORecord<?> record = (ORecord<?>) o;
    if (record.getRecord().getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        o = record.<ORecord> load();
      } catch (ORecordNotFoundException e) {
        throw new OException("Error during loading record with id : " + record.getIdentity());
      }
    }
    return o;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (!(iRight instanceof OSQLFilterCondition) && !(iLeft instanceof OSQLFilterCondition))
      return OIndexReuseType.INDEX_METHOD;

    return OIndexReuseType.NO_INDEX;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object executeIndexQuery(OCommandContext iContext, OIndex<?> index, INDEX_OPERATION_TYPE iOperationType,
      List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    final Object result;

    if (indexDefinition.getParamCount() == 1) {
      if (!((indexDefinition instanceof OPropertyMapIndexDefinition) && ((OPropertyMapIndexDefinition) indexDefinition)
          .getIndexBy() == OPropertyMapIndexDefinition.INDEX_BY.VALUE))
        return null;

      final Object key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));

      if (key == null)
        return null;

      final Object indexResult = index.get(key);
      result = convertIndexResult(indexResult);
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

        if (fetchLimit > -1)
          result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
        else
          result = index.getValuesBetween(keyOne, true, keyTwo, true);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          final Object indexResult = index.get(keyOne);
          result = convertIndexResult(indexResult);
        } else
          return null;
      }

    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return result;
  }

  private Object convertIndexResult(Object indexResult) {
    Object result;
    if (indexResult instanceof Collection)
      result = (Collection<OIdentifiable>) indexResult;
    else if (indexResult == null)
      result = Collections.emptyList();
    else
      result = Collections.singletonList((OIdentifiable) indexResult);
    return result;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }
}
