/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
        for (final Object o : sourceCollection) {
          if (OQueryOperatorEquals.equals(iRight, o))
            return true;
        }
      }
    } else if (iRight instanceof Collection<?>) {

      final Collection<Object> sourceCollection = (Collection<Object>) iRight;
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

    return false;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<OIdentifiable> executeIndexQuery(OIndex<?> index, List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Collection<OIdentifiable> result;

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      final List<Object> inParams = (List<Object>) keyParams.get(0);
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

      if (fetchLimit > -1)
        result = index.getValues(inKeys, fetchLimit);
      else
        result = index.getValues(inKeys);
    } else
      return null;

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

    final List<ORID> rids = new ArrayList<ORID>(ridSize);
    for (final Object rid : ridCollection) {
      if (rid instanceof ORID)
        rids.add((ORID) rid);
      else if (rid instanceof OSQLFilterItemParameter && ((OSQLFilterItemParameter) rid).getValue(null, null) instanceof ORID)
        rids.add((ORID) ((OSQLFilterItemParameter) rid).getValue(null, null));
    }

    return Collections.min(rids);
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

    final List<ORID> rids = new ArrayList<ORID>(ridSize);
    for (final Object rid : ridCollection) {
      if (rid instanceof ORID)
        rids.add((ORID) rid);
      else if (rid instanceof OSQLFilterItemParameter && ((OSQLFilterItemParameter) rid).getValue(null, null) instanceof ORID)
        rids.add((ORID) ((OSQLFilterItemParameter) rid).getValue(null, null));
    }

    return Collections.max(rids);
  }
}
