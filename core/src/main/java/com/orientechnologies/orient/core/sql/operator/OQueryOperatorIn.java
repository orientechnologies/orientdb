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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;

/**
 * IN operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorIn extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorIn() {
    super("IN", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    if (iLeft instanceof Collection<?>) {
      final Collection<Object> sourceCollection = (Collection<Object>) iLeft;

      if (iRight instanceof Collection<?>) {
        // AGAINST COLLECTION OF ITEMS
        final Collection<Object> collectionToMatch = (Collection<Object>) iRight;
        for (final Object o1 : sourceCollection) {
          for (final Object o2 : collectionToMatch) {
            if (OQueryOperatorEquals.equals(o1, o2))
              return true;
          }
        }
      } else {
        // AGAINST SINGLE ITEM
        if (sourceCollection instanceof Set<?>)
          return sourceCollection.contains(iRight);

        for (final Object o : sourceCollection) {
          if (OQueryOperatorEquals.equals(iRight, o))
            return true;
        }
      }
    } else if (iRight instanceof Collection<?>) {

      final Collection<Object> sourceCollection = (Collection<Object>) iRight;

      if (sourceCollection instanceof Set<?>)
        return sourceCollection.contains(iLeft);

      for (final Object o : sourceCollection) {
        if (OQueryOperatorEquals.equals(iLeft, o))
          return true;
      }
    } else if (iLeft.getClass().isArray()) {

      for (final Object o : (Object[]) iLeft) {
        if (OQueryOperatorEquals.equals(iRight, o))
          return true;
      }
    } else if (iRight.getClass().isArray()) {

      for (final Object o : (Object[]) iRight) {
        if (OQueryOperatorEquals.equals(iLeft, o))
          return true;
      }
    }

    return iLeft.equals(iRight);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object executeIndexQuery(OCommandContext iContext, OIndex<?> index, INDEX_OPERATION_TYPE iOperationType,
      List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Object result;

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      final Object inKeyValue = keyParams.get(0);
      final List<Object> inParams;
      if (inKeyValue instanceof List<?>)
        inParams = (List<Object>) inKeyValue;
      else if (inKeyValue instanceof OSQLFilterItem)
        inParams = (List<Object>) ((OSQLFilterItem) inKeyValue).getValue(null, iContext);
      else
        throw new IllegalArgumentException("Key '" + inKeyValue + "' is not valid");

      final List<Object> inKeys = new ArrayList<Object>();

      boolean containsNotCompatibleKey = false;
      for (final Object keyValue : inParams) {
        final Object key = indexDefinition.createValue(OSQLHelper.getValue(keyValue));
        if (key == null) {
          containsNotCompatibleKey = true;
          break;
        }

        inKeys.add(key);

      }
      if (containsNotCompatibleKey)
        return null;

      if (INDEX_OPERATION_TYPE.COUNT.equals(iOperationType))
        result = index.getValues(inKeys).size();
      else if (fetchLimit > -1)
        result = index.getValues(inKeys, fetchLimit);
      else
        result = index.getValues(inKeys);
    } else
      return null;

    updateProfiler(iContext, internalIndex, keyParams, indexDefinition);
    return result;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot())) {
      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else
      return null;

    final List<ORID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.min(rids);
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot())) {
      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else
      return null;

    final List<ORID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.max(rids);
  }

  protected List<ORID> addRangeResults(final Iterable<?> ridCollection, final int ridSize) {
    List<ORID> rids = null;
    for (Object rid : ridCollection) {
      if (rid instanceof OSQLFilterItemParameter)
        rid = ((OSQLFilterItemParameter) rid).getValue(null, null);

      if (rid instanceof OIdentifiable) {
        final ORID r = ((OIdentifiable) rid).getIdentity();
        if (r.isPersistent()) {
          if (rids == null)
            // LAZY CREATE IT
            rids = new ArrayList<ORID>(ridSize);
          rids.add(r);
        }
      }
    }
    return rids;
  }
}
