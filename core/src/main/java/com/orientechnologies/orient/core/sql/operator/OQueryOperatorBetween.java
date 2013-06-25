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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * BETWEEN operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorBetween extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorBetween() {
    super("BETWEEN", 5, false, 3);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    validate(iRight);

    final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

    final Object right1 = OType.convert(valueIterator.next(), iLeft.getClass());
    if (right1 == null)
      return false;
    valueIterator.next();
    final Object right2 = OType.convert(valueIterator.next(), iLeft.getClass());
    if (right2 == null)
      return false;

    return ((Comparable<Object>) iLeft).compareTo(right1) >= 0 && ((Comparable<Object>) iLeft).compareTo(right2) <= 0;
  }

  private void validate(Object iRight) {
    if (!OMultiValue.isMultiValue(iRight.getClass())) {
      throw new IllegalArgumentException("Found '" + iRight + "' while was expected: " + getSyntax());
    }

    if (OMultiValue.getSize(iRight) != 3)
      throw new IllegalArgumentException("Found '" + OMultiValue.toString(iRight) + "' while was expected: " + getSyntax());
  }

  @Override
  public String getSyntax() {
    return "<left> " + keyword + " <minRange> AND <maxRange>";
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Object executeIndexQuery(OCommandContext iContext, OIndex<?> index, INDEX_OPERATION_TYPE iOperationType,
      List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Object result;

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      final Object[] betweenKeys = (Object[]) keyParams.get(0);

      final Object keyOne = indexDefinition.createValue(Collections.singletonList(OSQLHelper.getValue(betweenKeys[0])));
      final Object keyTwo = indexDefinition.createValue(Collections.singletonList(OSQLHelper.getValue(betweenKeys[2])));

      if (keyOne == null || keyTwo == null)
        return null;

      if (iOperationType == INDEX_OPERATION_TYPE.COUNT)
        result = index.count(keyOne, true, keyTwo, true, fetchLimit);
      else {
        if (fetchLimit > -1)
          result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
        else
          result = index.getValuesBetween(keyOne, true, keyTwo, true);
      }

    } else {
      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      final Object[] betweenKeys = (Object[]) keyParams.get(keyParams.size() - 1);

      final Object betweenKeyOne = OSQLHelper.getValue(betweenKeys[0]);

      if (betweenKeyOne == null)
        return null;

      final Object betweenKeyTwo = OSQLHelper.getValue(betweenKeys[2]);

      if (betweenKeyTwo == null)
        return null;

      final List<Object> betweenKeyOneParams = new ArrayList<Object>(keyParams.size());
      betweenKeyOneParams.addAll(keyParams.subList(0, keyParams.size() - 1));
      betweenKeyOneParams.add(betweenKeyOne);

      final List<Object> betweenKeyTwoParams = new ArrayList<Object>(keyParams.size());
      betweenKeyTwoParams.addAll(keyParams.subList(0, keyParams.size() - 1));
      betweenKeyTwoParams.add(betweenKeyTwo);

      final Object keyOne = compositeIndexDefinition.createSingleValue(betweenKeyOneParams);

      if (keyOne == null)
        return null;

      final Object keyTwo = compositeIndexDefinition.createSingleValue(betweenKeyTwoParams);

      if (keyTwo == null)
        return null;

      if (iOperationType == INDEX_OPERATION_TYPE.COUNT)
        result = index.count(keyOne, true, keyTwo, true, fetchLimit);
      else {
        if (fetchLimit > -1)
          result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
        else
          result = index.getValuesBetween(keyOne, true, keyTwo, true);
      }
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return result;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    validate(iRight);

    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();
      if (right1 != null)
        return (ORID) right1;

      valueIterator.next();

      return (ORID) valueIterator.next();
    }

    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    validate(iRight);

    validate(iRight);

    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();

      valueIterator.next();

      final Object right2 = valueIterator.next();

      if (right2 == null)
        return (ORID) right1;

      return (ORID) right2;
    }

    return null;
  }
}
