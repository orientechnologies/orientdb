/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;

import java.util.Collection;
import java.util.List;

public class OLuceneOperatorUtil {

  public static boolean checkIndexExistence(final OClass iSchemaClass, final OIndexSearchResult result) {
    if (!iSchemaClass.areIndexed(result.fields()))
      return false;

    if (result.lastField.isLong()) {
      final int fieldCount = result.lastField.getItemCount();
      OClass cls = iSchemaClass.getProperty(result.lastField.getItemName(0)).getLinkedClass();

      for (int i = 1; i < fieldCount; i++) {
        if (cls == null || !cls.areIndexed(result.lastField.getItemName(i))) {
          return false;
        }

        cls = cls.getProperty(result.lastField.getItemName(i)).getLinkedClass();
      }
    }
    return true;
  }

  public static OIndexSearchResult createIndexedProperty(final OSQLFilterCondition iCondition, final Object iItem) {
    if (iItem == null || !(iItem instanceof OSQLFilterItemField))
      return null;

    if (iCondition.getLeft() instanceof OSQLFilterItemField && iCondition.getRight() instanceof OSQLFilterItemField)
      return null;

    final OSQLFilterItemField item = (OSQLFilterItemField) iItem;

    if (item.hasChainOperators() && !item.isFieldChain())
      return null;

    final Object origValue = iCondition.getLeft() == iItem ? iCondition.getRight() : iCondition.getLeft();

    if (iCondition.getOperator() instanceof OQueryOperatorBetween || iCondition.getOperator() instanceof OQueryOperatorIn) {
      return new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), origValue);
    }

    final Object value = OSQLHelper.getValue(origValue);

    if (value == null)
      return null;

    return new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), value);
  }

  public static OIndexSearchResult buildOIndexSearchResult(OClass iSchemaClass, OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults, OCommandContext context) {
    if (iCondition.getLeft() instanceof Collection) {
      OIndexSearchResult lastResult = null;

      Collection left = (Collection) iCondition.getLeft();
      int i = 0;
      Object lastValue = null;
      for (Object obj : left) {
        if (obj instanceof OSQLFilterItemField) {
          OSQLFilterItemField item = (OSQLFilterItemField) obj;

          Object value = null;
          if (iCondition.getRight() instanceof Collection) {
            List<Object> right = (List<Object>) iCondition.getRight();
            value = right.get(i);
          } else {
            value = iCondition.getRight();
          }
          if (lastResult == null) {
            lastResult = new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), value);
          } else {
            lastResult = lastResult.merge(new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), value));
          }

        } else if (obj instanceof OSQLFilterItemVariable) {
          OSQLFilterItemVariable item = (OSQLFilterItemVariable) obj;
          Object value = null;
          if (iCondition.getRight() instanceof Collection) {
            List<Object> right = (List<Object>) iCondition.getRight();
            value = right.get(i);
          } else {
            value = iCondition.getRight();
          }
          context.setVariable(item.toString(), value);
        }
        i++;
      }
      if (lastResult != null && OLuceneOperatorUtil.checkIndexExistence(iSchemaClass, lastResult))
        iIndexSearchResults.add(lastResult);
      return lastResult;
    } else {
      OIndexSearchResult result = OLuceneOperatorUtil.createIndexedProperty(iCondition, iCondition.getLeft());
      if (result == null)
        result = OLuceneOperatorUtil.createIndexedProperty(iCondition, iCondition.getRight());

      if (result == null)
        return null;

      if (OLuceneOperatorUtil.checkIndexExistence(iSchemaClass, result))
        iIndexSearchResults.add(result);

      return result;
    }
  }

}
