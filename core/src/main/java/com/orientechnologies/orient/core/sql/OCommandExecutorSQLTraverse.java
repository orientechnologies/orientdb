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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Executes a TRAVERSE crossing records. Returns a List<OIdentifiable> containing all the traversed
 * records that match the WHERE condition.
 *
 * <p>SYNTAX: <code>TRAVERSE <field>* FROM <target> WHERE <condition></code>
 *
 * <p>In the command context you've access to the variable $depth containing the depth level from
 * the root node. This is useful to limit the traverse up to a level. For example to consider from
 * the first depth level (0 is root node) to the third use: <code>
 * TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3</code>. To filter traversed records use
 * it combined with a SELECT statement:
 *
 * <p><code>
 * SELECT FROM (TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3) WHERE city.name = 'Rome'
 * </code>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLTraverse extends OCommandExecutorSQLResultsetAbstract {
  public static final String KEYWORD_WHILE = "WHILE";
  public static final String KEYWORD_TRAVERSE = "TRAVERSE";
  public static final String KEYWORD_STRATEGY = "STRATEGY";
  public static final String KEYWORD_MAXDEPTH = "MAXDEPTH";

  // HANDLES ITERATION IN LAZY WAY
  private OTraverse traverse = new OTraverse();

  /** Compile the filter conditions only the first time. */
  public OCommandExecutorSQLTraverse parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;
    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      super.parse(iRequest);

      final int pos = parseFields();
      if (pos == -1)
        throw new OCommandSQLParsingException(
            "Traverse must have the field list. Use " + getSyntax());
      parserSetCurrentPosition(pos);

      int endPosition = parserText.length();

      parsedTarget =
          OSQLEngine.getInstance()
              .parseTarget(parserText.substring(pos, endPosition), getContext());

      if (parsedTarget.parserIsEnded()) parserSetCurrentPosition(endPosition);
      else parserMoveCurrentPosition(parsedTarget.parserGetCurrentPosition());

      if (!parserIsEnded()) {
        parserNextWord(true);

        if (parserGetLastWord().equalsIgnoreCase(KEYWORD_WHERE))
          // // TODO Remove the additional management of WHERE for TRAVERSE after a while
          warnDeprecatedWhere();

        if (parserGetLastWord().equalsIgnoreCase(KEYWORD_WHERE)
            || parserGetLastWord().equalsIgnoreCase(KEYWORD_WHILE)) {

          compiledFilter =
              OSQLEngine.getInstance()
                  .parseCondition(
                      parserText.substring(parserGetCurrentPosition(), endPosition),
                      getContext(),
                      KEYWORD_WHILE);

          traverse.predicate(compiledFilter);
          optimize();
          int position;
          if (compiledFilter.parserIsEnded()) {
            position = endPosition;
          } else {
            position = compiledFilter.parserGetCurrentPosition() + parserGetCurrentPosition();
          }
          parserSetCurrentPosition(position);
        } else parserGoBack();
      }

      parserSkipWhiteSpaces();

      while (!parserIsEnded()) {
        if (parserOptionalKeyword(
            KEYWORD_LIMIT,
            KEYWORD_SKIP,
            KEYWORD_OFFSET,
            KEYWORD_TIMEOUT,
            KEYWORD_MAXDEPTH,
            KEYWORD_STRATEGY)) {
          final String w = parserGetLastWord();
          if (w.equals(KEYWORD_LIMIT)) parseLimit(w);
          else if (w.equals(KEYWORD_SKIP) || w.equals(KEYWORD_OFFSET)) parseSkip(w);
          else if (w.equals(KEYWORD_TIMEOUT)) parseTimeout(w);
          else if (w.equals(KEYWORD_MAXDEPTH)) parseMaxDepth(w);
          else if (w.equals(KEYWORD_STRATEGY)) parseStrategy(w);
        }
      }

      if (limit == 0 || limit < -1)
        throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
      else traverse.limit(limit);

      traverse.getContext().setParent(iRequest.getContext());
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  protected boolean parseMaxDepth(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_MAXDEPTH)) return false;

    String word = parserNextWord(true);

    try {
      traverse.setMaxDepth(Integer.parseInt(word));
    } catch (Exception ignore) {
      throwParsingException(
          "Invalid "
              + KEYWORD_MAXDEPTH
              + " value set to '"
              + word
              + "' but it should be a valid long. Example: "
              + KEYWORD_MAXDEPTH
              + " 3000");
    }

    if (traverse.getMaxDepth() < 0)
      throwParsingException(
          "Invalid "
              + KEYWORD_MAXDEPTH
              + ": value set minor than ZERO. Example: "
              + KEYWORD_MAXDEPTH
              + " 3");

    return true;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    context.beginExecution(timeoutMs, timeoutStrategy);

    if (!assignTarget(iArgs))
      throw new OQueryParsingException(
          "No source found in query: specify class, cluster(s) or single record(s)");

    try {
      // BROWSE ALL THE RECORDS AND COLLECTS RESULT
      final List<OIdentifiable> result = traverse.execute();
      for (OIdentifiable r : result)
        if (!handleResult(r, context))
          // LIMIT REACHED
          break;

      return getResult();
    } finally {
      request.getResultListener().end();
    }
  }

  @Override
  public OCommandContext getContext() {
    return traverse.getContext();
  }

  public Iterator<OIdentifiable> iterator() {
    return iterator(null);
  }

  public Iterator<OIdentifiable> iterator(final Map<Object, Object> iArgs) {
    assignTarget(iArgs);
    return traverse;
  }

  public String getSyntax() {
    return "TRAVERSE <field>* FROM <target> [MAXDEPTH <max-depth>] [WHILE <condition>] [STRATEGY <strategy>]";
  }

  protected void warnDeprecatedWhere() {
    OLogManager.instance()
        .warn(
            this,
            "Keyword WHERE in traverse has been replaced by WHILE. Please change your query to support WHILE instead of WHERE because now it's only deprecated, but in future it will be removed the back-ward compatibility.");
  }

  @Override
  protected boolean assignTarget(Map<Object, Object> iArgs) {
    if (super.assignTarget(iArgs)) {
      traverse.target(target);
      return true;
    }
    return false;
  }

  protected int parseFields() {
    int currentPos = 0;
    final StringBuilder word = new StringBuilder();

    currentPos = nextWord(parserText, parserTextUpperCase, currentPos, word, true);
    if (!word.toString().equals(KEYWORD_TRAVERSE)) return -1;

    int fromPosition = parserTextUpperCase.indexOf(KEYWORD_FROM_2FIND, currentPos);
    if (fromPosition == -1)
      throw new OQueryParsingException("Missed " + KEYWORD_FROM, parserText, currentPos);

    Set<Object> fields = new HashSet<Object>();

    final String fieldString = parserText.substring(currentPos, fromPosition).trim();
    if (fieldString.length() > 0) {
      // EXTRACT PROJECTIONS
      final List<String> items = OStringSerializerHelper.smartSplit(fieldString, ',');

      for (String field : items) {
        final String fieldName = field.trim();

        if (fieldName.contains("(")) fields.add(OSQLHelper.parseValue(null, fieldName, context));
        else fields.add(fieldName);
      }
    } else
      throw new OQueryParsingException(
          "Missed field list to cross in TRAVERSE. Use " + getSyntax(), parserText, currentPos);

    currentPos = fromPosition + KEYWORD_FROM.length() + 1;

    traverse.fields(fields);

    return currentPos;
  }

  /** Parses the strategy keyword if found. */
  protected boolean parseStrategy(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_STRATEGY)) return false;

    final String strategyWord = parserNextWord(true);

    try {
      traverse.setStrategy(OTraverse.STRATEGY.valueOf(strategyWord.toUpperCase(Locale.ENGLISH)));
    } catch (IllegalArgumentException ignore) {
      throwParsingException(
          "Invalid "
              + KEYWORD_STRATEGY
              + ". Use one between "
              + Arrays.toString(OTraverse.STRATEGY.values()));
    }
    return true;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.READ;
  }
}
