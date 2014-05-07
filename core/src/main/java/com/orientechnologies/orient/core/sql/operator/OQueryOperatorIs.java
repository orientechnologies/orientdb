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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

import java.util.Collection;
import java.util.List;

/**
 * IS operator. Different by EQUALS since works also for null. Example "IS null"
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorIs extends OQueryOperatorEquality {

  public OQueryOperatorIs() {
    super("IS", 5, false);
  }

  @Override
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    if (OSQLHelper.NOT_NULL.equals(iRight))
      return iLeft != null;
    else if (OSQLHelper.NOT_NULL.equals(iLeft))
      return iRight != null;
    else if (OSQLHelper.DEFINED.equals(iLeft))
      return evaluateDefined(iRecord, (String) iRight);
    else if (OSQLHelper.DEFINED.equals(iRight))
      return evaluateDefined(iRecord, (String) iLeft);
    else
      return iLeft == iRight;
  }

  protected boolean evaluateDefined(final OIdentifiable iRecord, final String iFieldName) {
    if (iRecord instanceof ODocument) {
      return ((ODocument) iRecord).containsField(iFieldName);
    }
    return false;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iRight == null)
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
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));
      else
        key = indexDefinition.createValue(keyParams);

      final Object indexResult;
      indexResult = index.get(key);

      if (indexResult == null || indexResult instanceof OIdentifiable)
        cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, key);
      else
        cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), key);
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams);
      final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams);

      if (internalIndex.hasRangeQuerySupport()) {
        cursor = index.iterateEntriesBetween(keyOne, true, keyTwo, true, ascSortOrder);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          final Object indexResult;
          indexResult = index.get(keyOne);

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

}
