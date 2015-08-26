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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexCursorCollectionValue;
import com.orientechnologies.orient.core.index.OIndexCursorSingleValue;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.INDEX_METHOD;
  }

  @SuppressWarnings("unchecked")
  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    OIndexCursor cursor;
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      final Object inKeyValue = keyParams.get(0);
      final Collection<Object> inParams;
      if (inKeyValue instanceof List<?>)
        inParams = (Collection<Object>) inKeyValue;
      else if (inKeyValue instanceof OSQLFilterItem)
        inParams = (Collection<Object>) ((OSQLFilterItem) inKeyValue).getValue(null, null, iContext);
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

      cursor = index.iterateEntries(inKeys, ascSortOrder);
    } else {
      final List<Object> partialKey = new ArrayList<Object>();
      partialKey.addAll(keyParams);
      partialKey.remove(keyParams.size() - 1);

      final Object inKeyValue = keyParams.get(keyParams.size() - 1);

      final Collection<Object> inParams;
      if (inKeyValue instanceof List<?>)
        inParams = (Collection<Object>) inKeyValue;
      else if (inKeyValue instanceof OSQLFilterItem)
        inParams = (Collection<Object>) ((OSQLFilterItem) inKeyValue).getValue(null, null, iContext);
      else
        throw new IllegalArgumentException("Key '" + inKeyValue + "' is not valid");

      final List<Object> inKeys = new ArrayList<Object>();

      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      boolean containsNotCompatibleKey = false;
      for (final Object keyValue : inParams) {
        List<Object> fullKey = new ArrayList<Object>();
        fullKey.addAll(partialKey);
        fullKey.add(keyValue);
        final Object key = compositeIndexDefinition.createSingleValue(fullKey);
        if (key == null) {
          containsNotCompatibleKey = true;
          break;
        }

        inKeys.add(key);

      }
      if (containsNotCompatibleKey) {
        return null;
      }

      if (inKeys == null)
        return null;

      if (indexDefinition.getParamCount() == keyParams.size()) {
        final Object indexResult;
        indexResult = index.iterateEntries(inKeys, ascSortOrder);

        if (indexResult == null || indexResult instanceof OIdentifiable) {
          cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, inKeys);
        } else if (indexResult instanceof OIndexCursor) {
          cursor = (OIndexCursor) indexResult;
        } else {
          cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), inKeys);
        }
      } else
        return null;
    }

    updateProfiler(iContext, internalIndex, keyParams, indexDefinition);
    return cursor;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot())) {
      if (iLeft instanceof OSQLFilterItem)
        iLeft = ((OSQLFilterItem) iLeft).getValue(null, null, null);

      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      if (iRight instanceof OSQLFilterItem)
        iRight = ((OSQLFilterItem) iRight).getValue(null, null, null);
      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else
      return null;

    final List<ORID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.min(rids);
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    final Iterable<?> ridCollection;
    final int ridSize;
    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot())) {
      if (iLeft instanceof OSQLFilterItem)
        iLeft = ((OSQLFilterItem) iLeft).getValue(null, null, null);

      ridCollection = OMultiValue.getMultiValueIterable(iLeft);
      ridSize = OMultiValue.getSize(iLeft);
    } else if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      if (iRight instanceof OSQLFilterItem)
        iRight = ((OSQLFilterItem) iRight).getValue(null, null, null);

      ridCollection = OMultiValue.getMultiValueIterable(iRight);
      ridSize = OMultiValue.getSize(iRight);
    } else
      return null;

    final List<ORID> rids = addRangeResults(ridCollection, ridSize);

    return rids == null ? null : Collections.max(rids);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    if (OMultiValue.isMultiValue(iLeft)) {
      if (iRight instanceof Collection<?>) {
        // AGAINST COLLECTION OF ITEMS
        final Collection<Object> collectionToMatch = (Collection<Object>) iRight;

        boolean found = false;
        for (final Object o1 : OMultiValue.getMultiValueIterable(iLeft)) {
          for (final Object o2 : collectionToMatch) {
            if (OQueryOperatorEquals.equals(o1, o2)) {
              found = true;
              break;
            }
          }
        }
        return found;
      } else {
        // AGAINST SINGLE ITEM
        if (iLeft instanceof Set<?>)
          return ((Set) iLeft).contains(iRight);

        for (final Object o : OMultiValue.getMultiValueIterable(iLeft)) {
          if (OQueryOperatorEquals.equals(iRight, o))
            return true;
        }
      }
    } else if (OMultiValue.isMultiValue(iRight)) {

      if (iRight instanceof Set<?>)
        return ((Set) iRight).contains(iLeft);

      for (final Object o : OMultiValue.getMultiValueIterable(iRight)) {
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

  protected List<ORID> addRangeResults(final Iterable<?> ridCollection, final int ridSize) {
    if (ridCollection == null)
      return null;

    List<ORID> rids = null;
    for (Object rid : ridCollection) {
      if (rid instanceof OSQLFilterItemParameter)
        rid = ((OSQLFilterItemParameter) rid).getValue(null, null, null);

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
