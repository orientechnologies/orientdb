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
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * CONTAINS KEY operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorContainsKey extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorContainsKey() {
    super("CONTAINSKEY", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {

    if (iLeft instanceof Map<?, ?>) {

      final Map<String, ?> map = (Map<String, ?>) iLeft;
      return map.containsKey(iRight);
    } else if (iRight instanceof Map<?, ?>) {

      final Map<String, ?> map = (Map<String, ?>) iRight;
      return map.containsKey(iLeft);
    }
    return false;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public boolean executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder,
																	 OIndex.IndexValuesResultListener resultListener) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return false;

    if (indexDefinition.getParamCount() == 1) {
      if (!((indexDefinition instanceof OPropertyMapIndexDefinition) && ((OPropertyMapIndexDefinition) indexDefinition)
          .getIndexBy() == OPropertyMapIndexDefinition.INDEX_BY.KEY))
        return false;

      final Object key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));

      if (key == null)
        return false;

      final Object indexResult = index.get(key);
      convertIndexResult(indexResult, resultListener);
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      if (!((compositeIndexDefinition.getMultiValueDefinition() instanceof OPropertyMapIndexDefinition) && ((OPropertyMapIndexDefinition) compositeIndexDefinition
          .getMultiValueDefinition()).getIndexBy() == OPropertyMapIndexDefinition.INDEX_BY.KEY))
        return false;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams);

      if (keyOne == null)
        return false;

      if (internalIndex.hasRangeQuerySupport()) {
        final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams);
        index.getValuesBetween(keyOne, true, keyTwo, true, ascSortOrder, resultListener);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          final Object indexResult = index.get(keyOne);
          convertIndexResult(indexResult, resultListener);
        } else
          return false;
      }
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return true;
  }

  private void convertIndexResult(Object indexResult, OIndex.IndexValuesResultListener resultListener) {
    if (indexResult instanceof Collection) {
      for (OIdentifiable identifiable : (Collection<OIdentifiable>) indexResult) {
        if (!resultListener.addResult(identifiable))
          return;
      }
    } else if (indexResult != null)
      resultListener.addResult((OIdentifiable) indexResult);
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
