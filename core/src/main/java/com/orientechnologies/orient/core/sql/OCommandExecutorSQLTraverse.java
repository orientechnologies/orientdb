/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Executes a TRAVERSE crossing records. Returns a List<OIdentifiable> containing all the traversed records that match the WHERE
 * condition.
 * <p>
 * SYNTAX: <code>TRAVERSE <field>* FROM <target> WHERE <condition></code>
 * </p>
 * <p>
 * In the command context you've access to the variable $depth containing the depth level from the root node. This is useful to
 * limit the traverse up to a level. For example to consider from the first depth level (0 is root node) to the third use:
 * <code>TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3</code>. To filter traversed records use it combined with a SELECT
 * statement:
 * </p>
 * <p>
 * <code>SELECT FROM (TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3) WHERE city.name = 'Rome'</code>
 * </p>
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLTraverse extends OCommandExecutorSQLResultsetAbstract {
  public static final String KEYWORD_WHILE    = "WHILE";
  public static final String KEYWORD_TRAVERSE = "TRAVERSE";

  // HANDLES ITERATION IN LAZY WAY
  private OTraverse          traverse         = new OTraverse();

  /**
   * Compile the filter conditions only the first time.
   */
  public OCommandExecutorSQLTraverse parse(final OCommandRequest iRequest) {
    super.parse(iRequest);

    final int pos = parseFields();
    if (pos == -1)
      throw new OCommandSQLParsingException("Traverse must have the field list. Use " + getSyntax());

    int endPosition = parserText.length();
    int endP = parserTextUpperCase.indexOf(" " + OCommandExecutorSQLTraverse.KEYWORD_LIMIT, parserGetCurrentPosition());
    if (endP > -1 && endP < endPosition)
      endPosition = endP;

    parsedTarget = OSQLEngine.getInstance().parseTarget(parserText.substring(pos, endPosition), getContext(), KEYWORD_WHILE);

    if (!parsedTarget.parserIsEnded()) {
      parserSetCurrentPosition(parsedTarget.parserGetCurrentPosition() + pos);

      parserNextWord(true);

      if (parserGetLastWord().equalsIgnoreCase(KEYWORD_WHERE))
        // // TODO Remove the additional management of WHERE for TRAVERSE after a while
        warnDeprecatedWhere();

      if (parserGetLastWord().equalsIgnoreCase(KEYWORD_WHERE) || parserGetLastWord().equalsIgnoreCase(KEYWORD_WHILE)) {

        compiledFilter = OSQLEngine.getInstance().parseCondition(parserText.substring(parserGetCurrentPosition(), endPosition),
            getContext(), KEYWORD_WHILE);

        traverse.predicate(compiledFilter);
        optimize();
        parserSetCurrentPosition(compiledFilter.parserIsEnded() ? endPosition : compiledFilter.parserGetCurrentPosition()
            + parserGetCurrentPosition());
      }
    } else
      parserSetCurrentPosition(-1);

    parserSkipWhiteSpaces();

    if (!parserIsEnded()) {
      if (parserOptionalKeyword(KEYWORD_LIMIT, KEYWORD_SKIP)) {
        final String w = parserGetLastWord();
        if (w.equals(KEYWORD_LIMIT))
          parseLimit(w);
        else if (w.equals(KEYWORD_SKIP))
          parseSkip(w);
      }
    }

    if (limit == 0 || limit < -1)
      throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
    else
      traverse.limit(limit);

    ((OCommandRequestText) iRequest).getContext().setChild(traverse.getContext());

    return this;
  }

  protected void warnDeprecatedWhere() {
    OLogManager
        .instance()
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

  public Object execute(final Map<Object, Object> iArgs) {
    if (!assignTarget(iArgs))
      throw new OQueryParsingException("No source found in query: specify class, cluster(s) or single record(s)");

    context = traverse.getContext();

    // BROWSE ALL THE RECORDS AND COLLECTS RESULT
    final List<OIdentifiable> result = (List<OIdentifiable>) traverse.execute();
    for (OIdentifiable r : result)
      handleResult(r);

    return getResult();
  }

  public boolean hasNext() {
    if (target == null)
      assignTarget(null);

    return traverse.hasNext();
  }

  @Override
  public OCommandContext getContext() {
    return traverse.getContext();
  }

  public OIdentifiable next() {
    if (target == null)
      assignTarget(null);

    return traverse.next();
  }

  public void remove() {
    throw new UnsupportedOperationException("remove()");
  }

  public Iterator<OIdentifiable> iterator() {
    return this;
  }

  protected int parseFields() {
    int currentPos = 0;
    final StringBuilder word = new StringBuilder();

    currentPos = nextWord(parserText, parserTextUpperCase, currentPos, word, true);
    if (!word.toString().equals(KEYWORD_TRAVERSE))
      return -1;

    int fromPosition = parserTextUpperCase.indexOf(KEYWORD_FROM_2FIND, currentPos);
    if (fromPosition == -1)
      throw new OQueryParsingException("Missed " + KEYWORD_FROM, parserText, currentPos);

    Set<String> fields = new HashSet<String>();

    final String fieldString = parserText.substring(currentPos, fromPosition).trim();
    if (fieldString.length() > 0) {
      // EXTRACT PROJECTIONS
      final List<String> items = OStringSerializerHelper.smartSplit(fieldString, ',');

      for (String field : items)
        fields.add(field.trim());
    } else
      throw new OQueryParsingException("Missed field list to cross in TRAVERSE. Use " + getSyntax(), parserText, currentPos);

    currentPos = fromPosition + KEYWORD_FROM.length() + 1;

    traverse.fields(fields);

    return currentPos;
  }

  public String getSyntax() {
    return "TRAVERSE <field>* FROM <target> [WHILE <condition>]";
  }
}
