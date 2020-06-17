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

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsKey;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsValue;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Presents query subset in form of field1 = "field1 value" AND field2 = "field2 value" ... AND
 * fieldN anyOpetator "fieldN value"
 *
 * <p>Where pairs (field1, value1) ... (fieldn-1, valuen-1) are stored in {@link #fieldValuePairs}
 * map but last pair is stored in {@link #lastField} {@link #lastValue} properties and their
 * operator will be stored in {@link #lastOperator} property.
 *
 * <p>Such data structure is used because from composite index point of view any "field and value"
 * pairs can be reordered to match keys order that is used in index in case all fields and values
 * are related to each other using equals operator, but position of field - value pair that uses non
 * equals operator cannot be changed. Actually only one non-equals operator can be used for
 * composite index search and filed - value pair that uses this index should always be placed at
 * last position.
 */
public class OIndexSearchResult {
  public final Map<String, Object> fieldValuePairs = new HashMap<String, Object>(8);
  public final OQueryOperator lastOperator;
  public final OSQLFilterItemField.FieldChain lastField;
  public final Object lastValue;
  protected boolean containsNullValues;

  public OIndexSearchResult(
      final OQueryOperator lastOperator,
      final OSQLFilterItemField.FieldChain field,
      final Object value) {
    this.lastOperator = lastOperator;
    lastField = field;
    lastValue = value;

    containsNullValues = value == null;
  }

  public static boolean isIndexEqualityOperator(OQueryOperator queryOperator) {
    return queryOperator instanceof OQueryOperatorEquals
        || queryOperator instanceof OQueryOperatorContains
        || queryOperator instanceof OQueryOperatorContainsKey
        || queryOperator instanceof OQueryOperatorContainsValue;
  }

  /**
   * Combines two queries subset into one. This operation will be valid only if {@link
   * #canBeMerged(OIndexSearchResult)} method will return <code>true</code> for the same passed in
   * parameter.
   *
   * @param searchResult Query subset to merge.
   * @return New instance that presents merged query.
   */
  public OIndexSearchResult merge(final OIndexSearchResult searchResult) {
    // if (searchResult.lastOperator instanceof OQueryOperatorEquals) {
    if (searchResult.lastOperator instanceof OQueryOperatorEquals) {
      return mergeFields(this, searchResult);
    }
    if (lastOperator instanceof OQueryOperatorEquals) {
      return mergeFields(searchResult, this);
    }
    if (isIndexEqualityOperator(searchResult.lastOperator)) {
      return mergeFields(this, searchResult);
    }
    return mergeFields(searchResult, this);
  }

  private OIndexSearchResult mergeFields(
      OIndexSearchResult mainSearchResult, OIndexSearchResult searchResult) {
    OIndexSearchResult result =
        new OIndexSearchResult(
            mainSearchResult.lastOperator, mainSearchResult.lastField, mainSearchResult.lastValue);
    result.fieldValuePairs.putAll(searchResult.fieldValuePairs);
    result.fieldValuePairs.putAll(mainSearchResult.fieldValuePairs);
    result.fieldValuePairs.put(searchResult.lastField.getItemName(0), searchResult.lastValue);
    result.containsNullValues = searchResult.containsNullValues || this.containsNullValues;
    return result;
  }

  /**
   * @param searchResult Query subset is going to be merged with given one.
   * @return <code>true</code> if two query subsets can be merged.
   */
  boolean canBeMerged(final OIndexSearchResult searchResult) {
    if (lastField.isLong() || searchResult.lastField.isLong()) {
      return false;
    }
    if (!lastOperator.canBeMerged() || !searchResult.lastOperator.canBeMerged()) {
      return false;
    }
    return isIndexEqualityOperator(lastOperator)
        || isIndexEqualityOperator(searchResult.lastOperator);
  }

  public List<String> fields() {
    final List<String> result = new ArrayList<String>(fieldValuePairs.size() + 1);
    result.addAll(fieldValuePairs.keySet());
    result.add(lastField.getItemName(0));
    return result;
  }

  int getFieldCount() {
    return fieldValuePairs.size() + 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OIndexSearchResult that = (OIndexSearchResult) o;

    if (containsNullValues != that.containsNullValues) {
      return false;
    }
    for (Map.Entry<String, Object> entry : fieldValuePairs.entrySet()) {
      if (!that.fieldValuePairs.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }

    if (!lastField.equals(that.lastField)) {
      return false;
    }
    if (!lastOperator.equals(that.lastOperator)) {
      return false;
    }
    if (!lastValue.equals(that.lastValue)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = lastOperator.hashCode();

    for (Map.Entry<String, Object> entry : fieldValuePairs.entrySet()) {
      if (entry.getKey() != null) {
        result = 31 * result + entry.getKey().hashCode();
      }
      if (entry.getValue() != null) {
        result = 31 * result + entry.getValue().hashCode();
      }
    }

    if (lastField != null) {
      result = 31 * result + lastField.hashCode();
    }
    if (lastValue != null) {
      result = 31 * result + lastValue.hashCode();
    }

    result = 31 * result + (containsNullValues ? 1 : 0);
    return result;
  }
}
