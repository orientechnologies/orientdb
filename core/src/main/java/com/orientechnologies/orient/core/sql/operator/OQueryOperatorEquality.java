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

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAll;

/**
 * Base equality operator. It's an abstract class able to compare the equality between two values.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OQueryOperatorEquality extends OQueryOperator {

  protected OQueryOperatorEquality(final String iKeyword, final int iPrecedence, final boolean iLogical) {
    super(iKeyword, iPrecedence, false);
  }

  protected OQueryOperatorEquality(final String iKeyword, final int iPrecedence, final boolean iLogical,
      final int iExpectedRightWords) {
    super(iKeyword, iPrecedence, false, iExpectedRightWords);
  }

  protected OQueryOperatorEquality(final String iKeyword, final int iPrecedence, final boolean iLogical,
      final int iExpectedRightWords, final boolean iExpectsParameters) {
    super(iKeyword, iPrecedence, iLogical, iExpectedRightWords, iExpectsParameters);
  }

  protected abstract boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition,
      final Object iLeft, final Object iRight, OCommandContext iContext);

  @Override
  public Object evaluateRecord(final OIdentifiable iRecord, ODocument iCurrentResult, final OSQLFilterCondition iCondition,
      final Object iLeft, final Object iRight, OCommandContext iContext) {
    if (iLeft instanceof OQueryRuntimeValueMulti) {
      // LEFT = MULTI
      final OQueryRuntimeValueMulti left = (OQueryRuntimeValueMulti) iLeft;

      if (left.getValues().length == 0) {
          return false;
      }

      if (left.getDefinition().getRoot().startsWith(OSQLFilterItemFieldAll.NAME)) {
        // ALL VALUES
        for (int i = 0; i < left.getValues().length; ++i) {
          Object v = left.getValues()[i];
          Object r = iRight;

          final OCollate collate = left.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            r = collate.transform(iRight);
          }

          if (v == null || !evaluateExpression(iRecord, iCondition, v, r, iContext)) {
              return false;
          }
        }
        return true;
      } else {
        // ANY VALUES
        for (int i = 0; i < left.getValues().length; ++i) {
          Object v = left.getValues()[i];
          Object r = iRight;

          final OCollate collate = left.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            r = collate.transform(iRight);
          }

          if (v != null && evaluateExpression(iRecord, iCondition, v, r, iContext)) {
              return true;
          }
        }
        return false;
      }

    } else if (iRight instanceof OQueryRuntimeValueMulti) {
      // RIGHT = MULTI
      final OQueryRuntimeValueMulti right = (OQueryRuntimeValueMulti) iRight;

      if (right.getValues().length == 0) {
          return false;
      }

      if (right.getDefinition().getRoot().startsWith(OSQLFilterItemFieldAll.NAME)) {
        // ALL VALUES
        for (int i = 0; i < right.getValues().length; ++i) {
          Object v = right.getValues()[i];
          Object l = iLeft;

          final OCollate collate = right.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            l = collate.transform(iLeft);
          }

          if (v == null || !evaluateExpression(iRecord, iCondition, l, v, iContext)) {
              return false;
          }
        }
        return true;
      } else {
        // ANY VALUES
        for (int i = 0; i < right.getValues().length; ++i) {
          Object v = right.getValues()[i];
          Object l = iLeft;

          final OCollate collate = right.getCollate(i);
          if (collate != null) {
            v = collate.transform(v);
            l = collate.transform(iLeft);
          }

          if (v != null && evaluateExpression(iRecord, iCondition, l, v, iContext)) {
              return true;
          }
        }
        return false;
      }
    } else {
        // SINGLE SIMPLE ITEM
        return evaluateExpression(iRecord, iCondition, iLeft, iRight, iContext);
    }
  }
}
