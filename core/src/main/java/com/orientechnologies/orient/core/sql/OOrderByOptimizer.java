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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;

import java.util.List;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OOrderByOptimizer {
  boolean canBeUsedByOrderBy(OIndex<?> index, List<OPair<String, String>> orderedFields) {
    if (orderedFields.isEmpty())
      return false;

    if (!index.supportsOrderedIterations())
      return false;

    final OIndexDefinition definition = index.getDefinition();
    final List<String> fields = definition.getFields();
    final int endIndex = Math.min(fields.size(), orderedFields.size());

    final String firstOrder = orderedFields.get(0).getValue();
    for (int i = 0; i < endIndex; i++) {
      final OPair<String, String> pair = orderedFields.get(i);

      if (!firstOrder.equals(pair.getValue()))
        return false;

      final String orderFieldName = orderedFields.get(i).getKey().toLowerCase();
      final String indexFieldName = fields.get(i).toLowerCase();

      if (!orderFieldName.equals(indexFieldName))
        return false;
    }

    return true;
  }

  /**
   * checks if, given a list of "=" conditions and a set of ORDER BY fields
   *
   * @param index
   * @param equalsFilterFields
   * @param orderedFields
   * @return
   */
  boolean canBeUsedByOrderByAfterFilter(OIndex<?> index, List<String> equalsFilterFields,
      List<OPair<String, String>> orderedFields) {
    if (orderedFields.isEmpty())
      return false;

    if (!index.supportsOrderedIterations())
      return false;

    final OIndexDefinition definition = index.getDefinition();
    final List<String> indexFields = definition.getFields();
    int endIndex = Math.min(indexFields.size(), equalsFilterFields.size());

    final String firstOrder = orderedFields.get(0).getValue();

    //check that all the "equals" clauses are a prefix for the index
    for (int i = 0; i < endIndex; i++) {
      final String equalsFieldName = equalsFilterFields.get(i).toLowerCase();
      final String indexFieldName = indexFields.get(i).toLowerCase();
      if (!equalsFieldName.equals(indexFieldName))
        return false;
    }

    endIndex = Math.min(indexFields.size(), orderedFields.size() + equalsFilterFields.size());
    if (endIndex == equalsFilterFields.size()) {
      //the index is used only for filtering
      return false;
    }
    //check that after that prefix there all the Order By fields in the right order
    for (int i = equalsFilterFields.size(); i < endIndex; i++) {
      int fieldOrderInOrderByClause = i - equalsFilterFields.size();
      final OPair<String, String> pair = orderedFields.get(fieldOrderInOrderByClause);

      if (!firstOrder.equals(pair.getValue()))
        return false;

      final String orderFieldName = pair.getKey().toLowerCase();
      final String indexFieldName = indexFields.get(i).toLowerCase();

      if (!orderFieldName.equals(indexFieldName))
        return false;
    }

    return true;
  }
}
