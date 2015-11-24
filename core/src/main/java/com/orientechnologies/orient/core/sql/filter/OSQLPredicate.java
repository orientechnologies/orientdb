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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNot;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.*;

/**
 * Parses text in SQL format and build a tree of conditions.
 *
 * @author Luca Garulli
 *
 */
public class OSQLPredicate extends OBaseParser implements OCommandPredicate {
  protected Set<OProperty>                properties = new HashSet<OProperty>();
  protected OSQLFilterCondition           rootCondition;
  protected List<String>                  recordTransformed;
  protected List<OSQLFilterItemParameter> parameterItems;
  protected int                           braces;
  protected OCommandContext               context;

  public OSQLPredicate() {
  }

  public OSQLPredicate(final String iText) {
    text(iText);
  }

  protected void throwSyntaxErrorException(final String iText) {
    final String syntax = getSyntax();
    if (syntax.equals("?"))
      throw new OCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());

    throw new OCommandSQLParsingException(iText + ". Use " + syntax, parserText, parserGetPreviousPosition());
  }

  protected String upperCase(String text) {
    // TODO remove and refactor (see same method in OCommandExecutorAbstract)
    StringBuilder result = new StringBuilder(text.length());
    for (char c : text.toCharArray()) {
      String upper = ("" + c).toUpperCase(Locale.ENGLISH);
      if (upper.length() > 1) {
        result.append(c);
      } else {
        result.append(upper);
      }
    }
    return result.toString();
  }

  public OSQLPredicate text(final String iText) {
    if (iText == null)
      throw new OCommandSQLParsingException("Query text is null");

    try {
      parserText = iText;
      parserTextUpperCase = upperCase(parserText);
      parserSetCurrentPosition(0);
      parserSkipWhiteSpaces();

      rootCondition = (OSQLFilterCondition) extractConditions(null);

      optimize();
    } catch (OQueryParsingException e) {
      if (e.getText() == null)
        // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
        throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), e);

      throw e;
    } catch (Throwable t) {
      throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), t);
    }
    return this;
  }

  public Object evaluate() {
    return evaluate(null, null, null);
  }

  public Object evaluate(final OCommandContext iContext) {
    return evaluate(null, null, iContext);
  }

  public Object evaluate(final OIdentifiable iRecord, ODocument iCurrentResult, final OCommandContext iContext) {
    if (rootCondition == null)
      return true;

    return rootCondition.evaluate(iRecord, iCurrentResult, iContext);
  }

  protected Object extractConditions(final OSQLFilterCondition iParentCondition) {
    final int oldPosition = parserGetCurrentPosition();
    parserNextWord(true, " )=><,\r\n");
    final String word = parserGetLastWord();

    boolean inBraces = word.length() > 0 && word.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN;

    if (word.length() > 0 && (word.equalsIgnoreCase("SELECT") || word.equalsIgnoreCase("TRAVERSE"))) {
      // SUB QUERY
      final StringBuilder embedded = new StringBuilder(256);
      OStringSerializerHelper.getEmbedded(parserText, oldPosition - 1, -1, embedded);
      parserSetCurrentPosition(oldPosition + embedded.length() + 1);
      return new OSQLSynchQuery<Object>(embedded.toString());
    }

    parserSetCurrentPosition(oldPosition);
    OSQLFilterCondition currentCondition = extractCondition();

    // CHECK IF THERE IS ANOTHER CONDITION ON RIGHT
    while (parserSkipWhiteSpaces()) {

      if (!parserIsEnded() && parserGetCurrentChar() == ')')
        return currentCondition;

      final OQueryOperator nextOperator = extractConditionOperator();
      if (nextOperator == null)
        return currentCondition;

      if (nextOperator.precedence > currentCondition.getOperator().precedence) {
        // SWAP ITEMS
        final OSQLFilterCondition subCondition = new OSQLFilterCondition(currentCondition.right, nextOperator);
        currentCondition.right = subCondition;
        subCondition.right = extractConditionItem(false, 1);
      } else {
        final OSQLFilterCondition parentCondition = new OSQLFilterCondition(currentCondition, nextOperator);
        parentCondition.right = extractConditions(parentCondition);
        currentCondition = parentCondition;
      }
    }

    currentCondition.inBraces = inBraces;

    // END OF TEXT
    return currentCondition;
  }

  protected OSQLFilterCondition extractCondition() {

    if (!parserSkipWhiteSpaces())
      // END OF TEXT
      return null;

    // EXTRACT ITEMS
    Object left = extractConditionItem(true, 1);

    if (left != null && checkForEnd(left.toString()))
      return null;

    OQueryOperator oper;
    final Object right;

    if (left instanceof OQueryOperator && ((OQueryOperator) left).isUnary()) {
      oper = (OQueryOperator) left;
      left = extractConditionItem(false, 1);
      right = null;
    } else {
      oper = extractConditionOperator();

      if (oper instanceof OQueryOperatorNot)
        // SPECIAL CASE: READ NEXT OPERATOR
        oper = new OQueryOperatorNot(extractConditionOperator());

      right = oper != null ? extractConditionItem(false, oper.expectedRightWords) : null;
    }

    // CREATE THE CONDITION OBJECT
    return new OSQLFilterCondition(left, oper, right);
  }

  protected boolean checkForEnd(final String iWord) {
    if (iWord != null
        && (iWord.equals(OCommandExecutorSQLSelect.KEYWORD_ORDER) || iWord.equals(OCommandExecutorSQLSelect.KEYWORD_LIMIT)
            || iWord.equals(OCommandExecutorSQLSelect.KEYWORD_SKIP) || iWord.equals(OCommandExecutorSQLSelect.KEYWORD_OFFSET))) {
      parserMoveCurrentPosition(iWord.length() * -1);
      return true;
    }
    return false;
  }

  private OQueryOperator extractConditionOperator() {
    if (!parserSkipWhiteSpaces())
      // END OF PARSING: JUST RETURN
      return null;

    if (parserGetCurrentChar() == ')')
      // FOUND ')': JUST RETURN
      return null;

    final OQueryOperator[] operators = OSQLEngine.getInstance().getRecordOperators();
    final String[] candidateOperators = new String[operators.length];
    for (int i = 0; i < candidateOperators.length; ++i)
      candidateOperators[i] = operators[i].keyword;

    final int operatorPos = parserNextChars(true, false, candidateOperators);

    if (operatorPos == -1) {
      parserGoBack();
      return null;
    }

    final OQueryOperator op = operators[operatorPos];
    if (op.expectsParameters) {
      // PARSE PARAMETERS IF ANY
      parserGoBack();

      parserNextWord(true, " 0123456789'\"");
      final String word = parserGetLastWord();

      final List<String> params = new ArrayList<String>();
      // CHECK FOR PARAMETERS
      if (word.length() > op.keyword.length() && word.charAt(op.keyword.length()) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        int paramBeginPos = parserGetCurrentPosition() - (word.length() - op.keyword.length());
        parserSetCurrentPosition(OStringSerializerHelper.getParameters(parserText, paramBeginPos, -1, params));
      } else if (!word.equals(op.keyword))
        throw new OQueryParsingException("Malformed usage of operator '" + op.toString() + "'. Parsed operator is: " + word);

      try {
        // CONFIGURE COULD INSTANTIATE A NEW OBJECT: ACT AS A FACTORY
        return op.configure(params);
      } catch (Exception e) {
        throw new OQueryParsingException("Syntax error using the operator '" + op.toString() + "'. Syntax is: " + op.getSyntax(), e);
      }
    } else
      parserMoveCurrentPosition(+1);
    return op;
  }

  private Object extractConditionItem(final boolean iAllowOperator, final int iExpectedWords) {
    final Object[] result = new Object[iExpectedWords];

    for (int i = 0; i < iExpectedWords; ++i) {
      parserNextWord(false, " =><,\r\n");
      String word = parserGetLastWord();

      if (word.length() == 0)
        break;

      word = word.replaceAll("\\\\", "\\\\\\\\");

      final String uWord = word.toUpperCase();

      final int lastPosition = parserIsEnded() ? parserText.length() : parserGetCurrentPosition();

      if (word.length() > 0 && word.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        braces++;


        // SUB-CONDITION
        parserSetCurrentPosition(lastPosition - word.length() + 1);

        final Object subCondition = extractConditions(null);

        if (!parserSkipWhiteSpaces() || parserGetCurrentChar() == ')') {
          braces--;
          parserMoveCurrentPosition(+1);
        }
        if (subCondition instanceof OSQLFilterCondition) {
          ((OSQLFilterCondition) subCondition).inBraces = true;
        }
        result[i] = subCondition;
      } else if (word.charAt(0) == OStringSerializerHelper.LIST_BEGIN) {
        // COLLECTION OF ELEMENTS
        parserSetCurrentPosition(lastPosition - getLastWordLength());

        final List<String> stringItems = new ArrayList<String>();
        parserSetCurrentPosition(OStringSerializerHelper.getCollection(parserText, parserGetCurrentPosition(), stringItems));
        result[i] = convertCollectionItems(stringItems);

        parserMoveCurrentPosition(+1);

      } else if (uWord.startsWith(OSQLFilterItemFieldAll.NAME + OStringSerializerHelper.EMBEDDED_BEGIN)) {

        result[i] = new OSQLFilterItemFieldAll(this, word);

      } else if (uWord.startsWith(OSQLFilterItemFieldAny.NAME + OStringSerializerHelper.EMBEDDED_BEGIN)) {

        result[i] = new OSQLFilterItemFieldAny(this, word);

      } else {

        if (uWord.equals("NOT")) {
          if (iAllowOperator)
            return new OQueryOperatorNot();
          else {
            // GET THE NEXT VALUE
            parserNextWord(false, " )=><,\r\n");
            final String nextWord = parserGetLastWord();

            if (nextWord.length() > 0) {
              word += " " + nextWord;

              if (word.endsWith(")"))
                word = word.substring(0, word.length() - 1);
            }
          }
        } else if (uWord.equals("AND"))
          // SPECIAL CASE IN "BETWEEN X AND Y"
          result[i] = word;

        while (word.endsWith(")")) {
          final int openParenthesis = word.indexOf('(');
          if (openParenthesis == -1) {
            // DISCARD END PARENTHESIS
            word = word.substring(0, word.length() - 1);
            parserMoveCurrentPosition(-1);
          } else
            break;
        }

        word = word.replaceAll("\\\\\\\\", "\\\\");
        result[i] = OSQLHelper.parseValue(this, this, word, context);
      }
    }

    return iExpectedWords == 1 ? result[0] : result;
  }

  private List<Object> convertCollectionItems(List<String> stringItems) {
    List<Object> coll = new ArrayList<Object>();
    for (String s : stringItems) {
      coll.add(OSQLHelper.parseValue(this, this, s, context));
    }
    return coll;
  }

  public OSQLFilterCondition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    if (rootCondition != null)
      return "Parsed: " + rootCondition.toString();
    return "Unparsed: " + parserText;
  }

  /**
   * Binds parameters.
   */
  public void bindParameters(final Map<Object, Object> iArgs) {
    if (parameterItems == null || iArgs == null || iArgs.size() == 0)
      return;

    for (int i = 0; i < parameterItems.size(); i++) {
      OSQLFilterItemParameter value = parameterItems.get(i);
      if ("?".equals(value.getName())) {
        value.setValue(iArgs.get(i));
      } else {
        value.setValue(iArgs.get(value.getName()));
      }
    }
  }

  public OSQLFilterItemParameter addParameter(final String iName) {
    final String name;
    if (iName.charAt(0) == OStringSerializerHelper.PARAMETER_NAMED) {
      name = iName.substring(1);

      // CHECK THE PARAMETER NAME IS CORRECT
      if (!OStringSerializerHelper.isAlphanumeric(name)) {
        throw new OQueryParsingException("Parameter name '" + name + "' is invalid, only alphanumeric characters are allowed");
      }
    } else
      name = iName;

    final OSQLFilterItemParameter param = new OSQLFilterItemParameter(name);

    if (parameterItems == null)
      parameterItems = new ArrayList<OSQLFilterItemParameter>();

    parameterItems.add(param);
    return param;
  }

  public void setRootCondition(final OSQLFilterCondition iCondition) {
    rootCondition = iCondition;
  }

  protected void optimize() {
    if (rootCondition != null)
      computePrefetchFieldList(rootCondition, new HashSet<String>());
  }

  protected Set<String> computePrefetchFieldList(final OSQLFilterCondition iCondition, final Set<String> iFields) {
    Object left = iCondition.getLeft();
    Object right = iCondition.getRight();
    if (left instanceof OSQLFilterItemField) {
      ((OSQLFilterItemField) left).setPreLoadedFields(iFields);
      iFields.add(((OSQLFilterItemField) left).getRoot());
    } else if (left instanceof OSQLFilterCondition)
      computePrefetchFieldList((OSQLFilterCondition) left, iFields);

    if (right instanceof OSQLFilterItemField) {
      ((OSQLFilterItemField) right).setPreLoadedFields(iFields);
      iFields.add(((OSQLFilterItemField) right).getRoot());
    } else if (right instanceof OSQLFilterCondition)
      computePrefetchFieldList((OSQLFilterCondition) right, iFields);

    return iFields;
  }
}
