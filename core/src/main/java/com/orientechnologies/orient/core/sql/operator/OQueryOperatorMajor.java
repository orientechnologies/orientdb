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

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;

/**
 * MAJOR operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorMajor extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorMajor() {
    super(">", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    final Object right = OType.convert(iRight, iLeft.getClass());
    if (right == null)
      return false;
    return ((Comparable<Object>) iLeft).compareTo(right) > 0;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iRight == null || iLeft == null)
      return OIndexReuseType.NO_INDEX;
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Object executeIndexQuery(OCommandContext iContext, OIndex<?> index, INDEX_OPERATION_TYPE iOperationType,
      List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators() || !internalIndex.hasRangeQuerySupport())
      return null;

    final Object result;

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));
      else
        key = indexDefinition.createValue(keyParams);

      if (key == null)
        return null;

      if (INDEX_OPERATION_TYPE.COUNT.equals(iOperationType))
        result = index.count(key, false, null, false, fetchLimit);
      else if (fetchLimit > -1)
        result = index.getValuesMajor(key, false, fetchLimit);
      else
        result = index.getValuesMajor(key, false);
    } else {
      // if we have situation like "field1 = 1 AND field2 > 2"
      // then we fetch collection which left not included boundary is the smallest composite key in the
      // index that contains keys with values field1=1 and field2=2 and which right included boundary
      // is the biggest composite key in the index that contains key with value field1=1.

      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams);

      if (keyOne == null)
        return null;

      final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams.subList(0, keyParams.size() - 1));

      if (keyTwo == null)
        return null;

      if (INDEX_OPERATION_TYPE.COUNT.equals(iOperationType))
        result = index.count(keyOne, false, keyTwo, true, fetchLimit);
      else if (fetchLimit > -1)
        result = index.getValuesBetween(keyOne, false, keyTwo, true, fetchLimit);
      else
        result = index.getValuesBetween(keyOne, false, keyTwo, true);
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return result;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot()))
      if (iRight instanceof ORID)
        return new ORecordId(((ORID) iRight).next());
      else {
        if (iRight instanceof OSQLFilterItemParameter && ((OSQLFilterItemParameter) iRight).getValue(null, null) instanceof ORID)
          return new ORecordId(((ORID) ((OSQLFilterItemParameter) iRight).getValue(null, null)).next());
      }
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }
}
