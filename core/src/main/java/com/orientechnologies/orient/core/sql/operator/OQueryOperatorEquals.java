/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * EQUALS operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorEquals extends OQueryOperatorEqualityNotNulls {

  private boolean binaryEvaluate = false;

  public OQueryOperatorEquals() {
    super("=", 5, false);
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) binaryEvaluate = db.getSerializer().getSupportBinaryEvaluate();
  }

  public static boolean equals(final Object iLeft, final Object iRight, OType type) {
    if (type == null) {
      return equals(iLeft, iRight);
    }
    Object left = OType.convert(iLeft, type.getDefaultJavaType());
    Object right = OType.convert(iRight, type.getDefaultJavaType());
    return equals(left, right);
  }

  public static boolean equals(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null) return false;

    if (iLeft == iRight) {
      return true;
    }

    if (iLeft instanceof OResult && !(iRight instanceof OResult)) {
      iLeft = ((OResult) iLeft).toElement();
    }

    if (iRight instanceof OResult && !(iLeft instanceof OResult)) {
      iRight = ((OResult) iRight).toElement();
    }

    // RECORD & ORID
    if (iLeft instanceof ORecord) return comparesValues(iRight, (ORecord) iLeft, true);
    else if (iRight instanceof ORecord) return comparesValues(iLeft, (ORecord) iRight, true);
    else if (iRight instanceof OResult) {
      return comparesValues(iLeft, (OResult) iRight, true);
    }

    // NUMBERS
    if (iLeft instanceof Number && iRight instanceof Number) {
      Number[] couple = OType.castComparableNumber((Number) iLeft, (Number) iRight);
      return couple[0].equals(couple[1]);
    }

    // ALL OTHER CASES
    try {
      final Object right = OType.convert(iRight, iLeft.getClass());

      if (right == null) return false;
      if (iLeft instanceof byte[] && iRight instanceof byte[]) {
        return Arrays.equals((byte[]) iLeft, (byte[]) iRight);
      }
      return iLeft.equals(right);
    } catch (Exception ignore) {
      return false;
    }
  }

  protected static boolean comparesValues(
      final Object iValue, final ORecord iRecord, final boolean iConsiderIn) {
    // ORID && RECORD
    final ORID other = ((ORecord) iRecord).getIdentity();

    if (!other.isPersistent() && iRecord instanceof ODocument) {
      // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
      final Set<String> firstFieldName = ((ODocument) iRecord).getPropertyNames();
      if (firstFieldName.size() > 0) {
        Object fieldValue = ((ODocument) iRecord).getProperty(firstFieldName.iterator().next());
        if (fieldValue != null) {
          if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue, false)) {
              if (o != null && o.equals(iValue)) return true;
            }
          }

          return fieldValue.equals(iValue);
        }
      }
      return false;
    }
    return other.equals(iValue);
  }

  protected static boolean comparesValues(
      final Object iValue, final OResult iRecord, final boolean iConsiderIn) {
    // ODOCUMENT AS RESULT OF SUB-QUERY: GET THE FIRST FIELD IF ANY
    Set<String> firstFieldName = iRecord.getPropertyNames();
    if (firstFieldName.size() == 1) {
      Object fieldValue = iRecord.getProperty(firstFieldName.iterator().next());
      if (fieldValue != null) {
        if (iConsiderIn && OMultiValue.isMultiValue(fieldValue)) {
          for (Object o : OMultiValue.getMultiValueIterable(fieldValue, false)) {
            if (o != null && o.equals(iValue)) return true;
          }
        }

        return fieldValue.equals(iValue);
      }
    }
    return false;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OIdentifiable && iRight instanceof OIdentifiable)
      return OIndexReuseType.NO_INDEX;
    if (iRight == null || iLeft == null) return OIndexReuseType.NO_INDEX;

    return OIndexReuseType.INDEX_METHOD;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final OIndexInternal internalIndex = index.getInternal();
    Stream<ORawPair<Object, ORID>> stream;
    if (!internalIndex.canBeUsedInEqualityOperators()) return null;

    if (indexDefinition.getParamCount() == 1) {
      final Object key;
      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        key = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(keyParams.get(0));
      else key = indexDefinition.createValue(keyParams);

      if (key == null) return null;

      stream = index.getInternal().getRids(key).map((rid) -> new ORawPair<>(key, rid));
    } else {
      // in case of composite keys several items can be returned in case of we perform search
      // using part of composite key stored in index.

      final OCompositeIndexDefinition compositeIndexDefinition =
          (OCompositeIndexDefinition) indexDefinition;

      final Object keyOne = compositeIndexDefinition.createSingleValue(keyParams);

      if (keyOne == null) return null;

      final Object keyTwo = compositeIndexDefinition.createSingleValue(keyParams);

      if (internalIndex.hasRangeQuerySupport()) {
        stream = index.getInternal().streamEntriesBetween(keyOne, true, keyTwo, true, ascSortOrder);
      } else {
        if (indexDefinition.getParamCount() == keyParams.size()) {
          stream = index.getInternal().getRids(keyOne).map((rid) -> new ORawPair<>(keyOne, rid));
        } else return null;
      }
    }

    updateProfiler(iContext, index, keyParams, indexDefinition);
    return stream;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    if (iLeft instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot()))
      if (iRight instanceof ORID) return (ORID) iRight;
      else {
        if (iRight instanceof OSQLFilterItemParameter
            && ((OSQLFilterItemParameter) iRight).getValue(null, null, null) instanceof ORID)
          return (ORID) ((OSQLFilterItemParameter) iRight).getValue(null, null, null);
      }

    if (iRight instanceof OSQLFilterItemField
        && ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iRight).getRoot()))
      if (iLeft instanceof ORID) return (ORID) iLeft;
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
  protected boolean evaluateExpression(
      final OIdentifiable iRecord,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext) {
    return equals(iLeft, iRight);
  }

  @Override
  public boolean evaluate(
      final OBinaryField iFirstField,
      final OBinaryField iSecondField,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    return serializer.getComparator().isEqual(iFirstField, iSecondField);
  }

  @Override
  public boolean isSupportingBinaryEvaluate() {
    return binaryEvaluate;
  }
}
