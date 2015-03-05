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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;

import java.util.List;

/**
 * MINOR EQUALS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorMinorEquals extends OQueryOperatorEqualityNotNulls {

  public OQueryOperatorMinorEquals() {
    super("<=", 5, false);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
      final Object iRight, OCommandContext iContext) {
    final Object right = OType.convert(iRight, iLeft.getClass());
    if (right == null) {
        return false;
    }
    return ((Comparable<Object>) iLeft).compareTo(right) <= 0;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iRight == null || iLeft == null) {
        return OIndexReuseType.NO_INDEX;
    }
    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal<?> internalIndex = index.getInternal();
    OIndexCursor cursor;
    if (!internalIndex.canBeUsedInEqualityOperators() || !internalIndex.hasRangeQuerySupport()) {
        return null;
    }

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue) {
          key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));
      } else {
          key = indexDefinition.createValue(keyParams);
      }

      if (key == null) {
          return null;
      }

      cursor = index.iterateEntriesMinor(key, true, ascSortOrder);
    } else {
      // if we have situation like "field1 = 1 AND field2 <= 2"
      // then we fetch collection which left included boundary is the smallest composite key in the
      // index that contains key with value field1=1 and which right not included boundary
      // is the biggest composite key in the index that contains key with value field1=1 and field2=2.

      final OCompositeIndexDefinition compositeIndexDefinition = (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams.subList(0, keyParams.size() - 1));

      if (keyOne == null) {
          return null;
      }

      final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams);

      if (keyTwo == null) {
          return null;
      }

      cursor = index.iterateEntriesBetween(keyOne, true, keyTwo, true, ascSortOrder);
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return cursor;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
        if (iRight instanceof ORID) {
            return (ORID) iRight;
        } else {
            if (iRight instanceof OSQLFilterItemParameter
                    && ((OSQLFilterItemParameter) iRight).getValue(null, null, null) instanceof ORID) {
                return (ORID) ((OSQLFilterItemParameter) iRight).getValue(null, null, null);
            }
        }
    }

    return null;
  }
}
