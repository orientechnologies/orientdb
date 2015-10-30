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

import java.util.Collection;
import java.util.List;

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

  public static boolean equals(final Object iLeft, final Object iRight, OType type) {
    if(type==null){
      return equals(iLeft, iRight);
    }
    Object left = OType.convert(iLeft, type.getDefaultJavaType());
    Object right = OType.convert(iRight, type.getDefaultJavaType());
    return equals(left, right);
  }

  public static boolean equals(final Object iLeft, final Object iRight) {
    if (iLeft == null || iRight == null)
      return false;

    // RECORD & ORID
    if (iLeft instanceof ORecord)
      return comparesValues(iRight, (ORecord) iLeft, true);
    else if (iRight instanceof ORecord)
      return comparesValues(iLeft, (ORecord) iRight, true);

    // ALL OTHER CASES
    final Object right = OType.convert(iRight, iLeft.getClass());
    if (right == null)
      return false;
    return iLeft.equals(right);
  }

  protected static boolean comparesValues(final Object iValue, final ORecord iRecord, final boolean iConsiderIn) {
    // ORID && RECORD
    final ORID other = ((ORecord) iRecord).getIdentity();

    if (!other.isPersistent() && iRecord instanceof ODocument) {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final String[] firstFieldName = ((ODocument) iRecord).fieldNames();
      if (firstFieldName.length > 0) {
        Object fieldValue = ((ODocument) iRecord).field(firstFieldName[0]);
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue)) {
              if (o != null && o.equals(iValue))
                return true;
            }
          }

          return fieldValue.equals(iValue);
        }
      }
      return false;
    }
    return other.equals(iValue);
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OIdentifiable && iRight instanceof OIdentifiable)
      return OIndexReuseType.NO_INDEX;
    if (iRight == null || iLeft == null)
      return OIndexReuseType.NO_INDEX;

    return OIndexReuseType.INDEX_METHOD;
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

      if (key == null)
        return null;

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

      if (keyOne == null)
        return null;

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
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot()))
      if (iRight instanceof ORID)
        return (ORID) iRight;
      else {
        if (iRight instanceof OSQLFilterItemParameter
            && ((OSQLFilterItemParameter) iRight).getValue(null, null, null) instanceof ORID)
          return (ORID) ((OSQLFilterItemParameter) iRight).getValue(null, null, null);
      }

    if (iRight instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot()))
      if (iLeft instanceof ORID)
        return (ORID) iLeft;
      else {
        if (iLeft instanceof OSQLFilterItemParameter
            && ((OSQLFilterItemParameter) iLeft).getValue(null, null, null) instanceof ORID)
          return (ORID) ((OSQLFilterItemParameter) iLeft).getValue(null, null, null);
      }

    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    return getBeginRidRange(iLeft, iRight);
  }

  @Override
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    return equals(iLeft, iRight);
  }
}
