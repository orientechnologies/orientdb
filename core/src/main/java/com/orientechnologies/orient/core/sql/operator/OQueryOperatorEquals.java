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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;

/**
 * EQUALS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorEquals extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorEquals() {
    super("=", 5, false);
  }

  @Override
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    return equals(iLeft, iRight);
  }

  public static boolean equals(final Object iLeft, final Object iRight) {
    if (iLeft instanceof ORecord<?> && iRight instanceof ORID)
      // RECORD & ORID
      return ((ORecord<?>) iLeft).getIdentity().equals(iRight);
    else if (iRight instanceof ORecord<?> && iLeft instanceof ORID)
      // ORID && RECORD
      return ((ORecord<?>) iRight).getIdentity().equals(iLeft);
    else if (iRight instanceof ODocument) {
      // MATCH WITH ONE SINGLE DOCUMENT FIELD
      final ODocument r = (ODocument) iRight;
      if (!r.getIdentity().isPersistent() && r.fields() == 1) {
        Object field = r.field(r.fieldNames()[0]);
        return iLeft.equals(field);
      }
    } else if (iLeft instanceof ODocument) {
      // MATCH WITH ONE SINGLE DOCUMENT FIELD
      final ODocument r = (ODocument) iLeft;
      if (!r.getIdentity().isPersistent() && r.fields() == 1) {
        Object field = r.field(r.fieldNames()[0]);
        return iRight.equals(field);
      }
    }

    // ALL OTHER CASES
    final Object right = OType.convert(iRight, iLeft.getClass());
    if (right == null)
      return false;
    return iLeft.equals(right);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OIdentifiable && iRight instanceof OIdentifiable)
      return OIndexReuseType.NO_INDEX;
    if (iRight == null || iLeft == null)
      return OIndexReuseType.NO_INDEX;

    return OIndexReuseType.INDEX_METHOD;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<OIdentifiable> executeIndexQuery(OIndex<?> index, List<Object> keyParams, int fetchLimit) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    if (!internalIndex.canBeUsedInEqualityOperators())
      return null;

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));
      else
        key = indexDefinition.createValue(keyParams);

      if (key == null)
        return null;

      final Object indexResult = index.get(key);
      if (indexResult instanceof Collection)
        return (Collection<OIdentifiable>) indexResult;

      if( indexResult == null )
        return null;
      
      return indexResult == null ? null : Collections.singletonList((OIdentifiable) indexResult);
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final Object keyOne = indexDefinition.createValue(keyParams);

      if (keyOne == null)
        return null;

      final Object keyTwo = indexDefinition.createValue(keyParams);

      final Collection<OIdentifiable> result;
      if (fetchLimit > -1)
        result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
      else
        result = index.getValuesBetween(keyOne, true, keyTwo, true);

      if (OProfiler.getInstance().isRecording()) {
        OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
        OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
      }

      return result;
    }
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot()))
      if (iRight instanceof ORID)
        return (ORID) iRight;
      else {
        if (iRight instanceof OSQLFilterItemParameter && ((OSQLFilterItemParameter) iRight).getValue(null, null) instanceof ORID)
          return (ORID) ((OSQLFilterItemParameter) iRight).getValue(null, null);
      }

    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot()))
      if (iLeft instanceof ORID)
        return (ORID) iLeft;
      else {
        if (iLeft instanceof OSQLFilterItemParameter && ((OSQLFilterItemParameter) iLeft).getValue(null, null) instanceof ORID)
          return (ORID) ((OSQLFilterItemParameter) iLeft).getValue(null, null);
      }

    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    return getBeginRidRange(iLeft, iRight);
  }
}
