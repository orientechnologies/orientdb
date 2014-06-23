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
 * Presents query subset in form of field1 = "field1 value" AND field2 = "field2 value" ... AND fieldN anyOpetator "fieldN value"
 * 
 * Where pairs (field1, value1) ... (fieldn-1, valuen-1) are stored in {@link #fieldValuePairs} map but last pair is stored in
 * {@link #lastField} {@link #lastValue} properties and their operator will be stored in {@link #lastOperator} property.
 * 
 * Such data structure is used because from composite index point of view any "field and value" pairs can be reordered to match keys
 * order that is used in index in case all fields and values are related to each other using equals operator, but position of field
 * - value pair that uses non equals operator cannot be changed. Actually only one non-equals operator can be used for composite
 * index search and filed - value pair that uses this index should always be placed at last position.
 */
public class OIndexSearchResult {
  final Map<String, Object>            fieldValuePairs = new HashMap<String, Object>(8);
  final OQueryOperator                 lastOperator;
  final OSQLFilterItemField.FieldChain lastField;
  final Object                         lastValue;
  boolean                              containsNullValues;

  public OIndexSearchResult(final OQueryOperator lastOperator, final OSQLFilterItemField.FieldChain field, final Object value) {
    this.lastOperator = lastOperator;
    lastField = field;
    lastValue = value;

    containsNullValues = value == null;
  }

  public static boolean isIndexEqualityOperator(OQueryOperator queryOperator) {
    return queryOperator instanceof OQueryOperatorEquals || queryOperator instanceof OQueryOperatorContains
        || queryOperator instanceof OQueryOperatorContainsKey || queryOperator instanceof OQueryOperatorContainsValue;
  }

  /**
   * Combines two queries subset into one. This operation will be valid only if {@link #canBeMerged(OIndexSearchResult)} method will
   * return <code>true</code> for the same passed in parameter.
   * 
   * @param searchResult
   *          Query subset to merge.
   * @return New instance that presents merged query.
   */
  public OIndexSearchResult merge(final OIndexSearchResult searchResult) {
    final OQueryOperator operator;
    final OIndexSearchResult result;

    if (searchResult.lastOperator instanceof OQueryOperatorEquals) {
      result = new OIndexSearchResult(this.lastOperator, lastField, lastValue);
      result.fieldValuePairs.putAll(searchResult.fieldValuePairs);
      result.fieldValuePairs.putAll(fieldValuePairs);
      result.fieldValuePairs.put(searchResult.lastField.getItemName(0), searchResult.lastValue);
    } else {
      operator = searchResult.lastOperator;
      result = new OIndexSearchResult(operator, searchResult.lastField, searchResult.lastValue);
      result.fieldValuePairs.putAll(searchResult.fieldValuePairs);
      result.fieldValuePairs.putAll(fieldValuePairs);
      result.fieldValuePairs.put(lastField.getItemName(0), lastValue);
    }

    result.containsNullValues = searchResult.containsNullValues || this.containsNullValues;
    return result;
  }

  /**
   * @param searchResult
   *          Query subset is going to be merged with given one.
   * @return <code>true</code> if two query subsets can be merged.
   */
  boolean canBeMerged(final OIndexSearchResult searchResult) {
    if (lastField.isLong() || searchResult.lastField.isLong()) {
      return false;
    }
    return isIndexEqualityOperator(lastOperator) || isIndexEqualityOperator(searchResult.lastOperator);
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

  public List<String> getInvolvedFields() {
    final List<String> list = new ArrayList<String>();
    list.add(lastField.getItemName(lastField.getItemCount() - 1));
    for (String f : fieldValuePairs.keySet())
      list.add(f);
    return list;
  }

  public OSQLFilterItemField.FieldChain getLastField() {
    return lastField;
  }
}
