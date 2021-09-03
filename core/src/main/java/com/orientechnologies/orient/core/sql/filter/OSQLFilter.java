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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Locale;

/**
 * Parsed query. It's built once a query is parsed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFilter extends OSQLPredicate implements OCommandPredicate {
  public OSQLFilter(
      final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    super();

    if (iText == null) {
      throw new IllegalArgumentException("Filter expression is null");
    }

    context = iContext;
    parserText = iText;
    parserTextUpperCase = iText.toUpperCase(Locale.ENGLISH);

    try {
      final int lastPos = parserGetCurrentPosition();
      final String lastText = parserText;
      final String lastTextUpperCase = parserTextUpperCase;

      text(parserText.substring(lastPos));

      parserText = lastText;
      parserTextUpperCase = lastTextUpperCase;
      parserMoveCurrentPosition(lastPos);

    } catch (OQueryParsingException e) {
      if (e.getText() == null)
      // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
      {
        throw OException.wrapException(
            new OQueryParsingException(
                "Error on parsing query", parserText, parserGetCurrentPosition()),
            e);
      }

      throw e;
    } catch (Exception e) {
      throw OException.wrapException(
          new OQueryParsingException(
              "Error on parsing query", parserText, parserGetCurrentPosition()),
          e);
    }

    this.rootCondition = resetOperatorPrecedence(rootCondition);
  }

  private OSQLFilterCondition resetOperatorPrecedence(OSQLFilterCondition iCondition) {
    if (iCondition == null) {
      return iCondition;
    }
    if (iCondition.left != null && iCondition.left instanceof OSQLFilterCondition) {
      iCondition.left = resetOperatorPrecedence((OSQLFilterCondition) iCondition.left);
    }

    if (iCondition.right != null && iCondition.right instanceof OSQLFilterCondition) {
      OSQLFilterCondition right = (OSQLFilterCondition) iCondition.right;
      iCondition.right = resetOperatorPrecedence(right);
      if (iCondition.operator != null) {
        if (!right.inBraces
            && right.operator != null
            && right.operator.precedence < iCondition.operator.precedence) {
          OSQLFilterCondition newLeft =
              new OSQLFilterCondition(iCondition.left, iCondition.operator, right.left);
          right.setLeft(newLeft);
          resetOperatorPrecedence(right);
          return right;
        }
      }
    }

    return iCondition;
  }

  public Object evaluate(
      final OIdentifiable iRecord, final ODocument iCurrentResult, final OCommandContext iContext) {
    if (rootCondition == null) {
      return true;
    }

    return rootCondition.evaluate(iRecord, iCurrentResult, iContext);
  }

  public OSQLFilterCondition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    if (rootCondition != null) {
      return "Parsed: " + rootCondition.toString();
    }
    return "Unparsed: " + parserText;
  }
}
