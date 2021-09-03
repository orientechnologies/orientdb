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

package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import java.util.Map;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OFilterOptimizer {
  public void optimize(OSQLFilter filter, OIndexSearchResult indexMatch) {
    filter.setRootCondition(optimize(filter.getRootCondition(), indexMatch));
  }

  private OSQLFilterCondition optimize(
      OSQLFilterCondition condition, OIndexSearchResult indexMatch) {
    if (condition == null) {
      return null;
    }
    OQueryOperator operator = condition.getOperator();
    while (operator == null) {
      if (condition.getRight() == null && condition.getLeft() instanceof OSQLFilterCondition) {
        condition = (OSQLFilterCondition) condition.getLeft();
        operator = condition.getOperator();
      } else {
        return condition;
      }
    }

    final OIndexReuseType reuseType =
        operator.getIndexReuseType(condition.getLeft(), condition.getRight());
    switch (reuseType) {
      case INDEX_METHOD:
        if (isCovered(indexMatch, operator, condition.getLeft(), condition.getRight())
            || isCovered(indexMatch, operator, condition.getRight(), condition.getLeft())) {
          return null;
        }
        return condition;

      case INDEX_INTERSECTION:
        if (condition.getLeft() instanceof OSQLFilterCondition)
          condition.setLeft(optimize((OSQLFilterCondition) condition.getLeft(), indexMatch));

        if (condition.getRight() instanceof OSQLFilterCondition)
          condition.setRight(optimize((OSQLFilterCondition) condition.getRight(), indexMatch));

        if (condition.getLeft() == null) return (OSQLFilterCondition) condition.getRight();
        if (condition.getRight() == null) return (OSQLFilterCondition) condition.getLeft();
        return condition;

      case INDEX_OPERATOR:
        if (isCovered(indexMatch, operator, condition.getLeft(), condition.getRight())
            || isCovered(indexMatch, operator, condition.getRight(), condition.getLeft())) {
          return null;
        }
        return condition;
      default:
        return condition;
    }
  }

  private boolean isCovered(
      OIndexSearchResult indexMatch,
      OQueryOperator operator,
      Object fieldCandidate,
      Object valueCandidate) {
    if (fieldCandidate instanceof OSQLFilterItemField) {
      final OSQLFilterItemField field = (OSQLFilterItemField) fieldCandidate;
      if (operator instanceof OQueryOperatorEquals)
        for (Map.Entry<String, Object> e : indexMatch.fieldValuePairs.entrySet()) {
          if (isSameField(field, e.getKey()) && isSameValue(valueCandidate, e.getValue()))
            return true;
        }

      return operator.equals(indexMatch.lastOperator)
          && isSameField(field, indexMatch.lastField)
          && isSameValue(valueCandidate, indexMatch.lastValue);
    }
    return false;
  }

  private boolean isSameValue(Object valueCandidate, Object lastValue) {
    if (lastValue == null || valueCandidate == null)
      return lastValue == null && valueCandidate == null;

    return lastValue.equals(valueCandidate)
        || lastValue.equals(OSQLHelper.getValue(valueCandidate));
  }

  private boolean isSameField(
      OSQLFilterItemField field, OSQLFilterItemField.FieldChain fieldChain) {
    return fieldChain.belongsTo(field);
  }

  private boolean isSameField(OSQLFilterItemField field, String fieldName) {
    return !field.hasChainOperators() && fieldName.equals(field.name);
  }
}
