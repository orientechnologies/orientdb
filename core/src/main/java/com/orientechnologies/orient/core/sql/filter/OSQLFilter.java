/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.*;

import java.util.*;

/**
 * Parsed query. It's built once a query is parsed.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilter extends OSQLPredicate implements OCommandPredicate {
  protected Iterable<? extends OIdentifiable> targetRecords;
  protected Map<String, String>               targetClusters;
  protected Map<OClass, String>               targetClasses;
  protected String                            targetIndex;

  public OSQLFilter(final String iText, final OCommandContext iContext) {
    super();
    context = iContext;
    text = iText;
    textUpperCase = iText.toUpperCase();

    try {
      if (extractTargets()) {
        // IF WHERE EXISTS EXTRACT CONDITIONS

        final StringBuilder word = new StringBuilder();
        int newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
        if (newPos > -1) {
          if (word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_WHERE)) {
            final int lastPos = newPos;
            final String lastText = text;
            final String lastTextUpperCase = textUpperCase;

            text(text.substring(newPos));

            if (currentPos > -1)
              currentPos += lastPos;
            text = lastText;
            textUpperCase = lastTextUpperCase;

          } else if (word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_LIMIT)
              || word.toString().equals(OCommandExecutorSQLSelect.KEYWORD_ORDER)
              || word.toString().equals(OCommandExecutorSQLSelect.KEYWORD_SKIP))
            return;
          else
            throw new OQueryParsingException("Found invalid keyword '" + word + "'", text, newPos);
        }
      }
    } catch (OQueryParsingException e) {
      if (e.getText() == null)
        // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
        throw new OQueryParsingException("Error on parsing query", text, currentPos, e);

      throw e;
    } catch (Throwable t) {
      throw new OQueryParsingException("Error on parsing query", text, currentPos, t);
    }
  }

  public boolean evaluate(final ORecord<?> iRecord, final OCommandContext iContext) {

    ORecordSchemaAware<?> recordSchemaAware = (ORecordSchemaAware<?>) iRecord;
    //check only classes that specified in query will go to result set
    if ((targetClasses != null)&&(!targetClasses.isEmpty())){
      for (OClass targetClass : targetClasses.keySet()){
        if (!targetClass.isSuperClassOf(recordSchemaAware.getSchemaClass()))
          return false;
      }
    }

    if (rootCondition == null)
      return true;

    return (Boolean) rootCondition.evaluate(recordSchemaAware, iContext);
  }

  @SuppressWarnings("unchecked")
  private boolean extractTargets() {
    currentPos = OStringParser.jumpWhiteSpaces(text, currentPos);

    if (currentPos == -1)
      throw new OQueryParsingException("No query target found", text, 0);

    final char c = text.charAt(currentPos);

    if (c == '#' || Character.isDigit(c)) {
      // UNIQUE RID
      final StringBuilder word = new StringBuilder();
      currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

      targetRecords = new ArrayList<OIdentifiable>();
      ((List<OIdentifiable>) targetRecords).add(new ORecordId(word.toString()));

    } else if (c == '(') {
      // SUB QUERY
      final StringBuilder subText = new StringBuilder();
      currentPos = OStringSerializerHelper.getEmbedded(text, currentPos, -1, subText);
      final OCommandSQL subCommand = new OCommandSQLResultset(subText.toString());

      final OCommandExecutor executor = OCommandManager.instance().getExecutor(subCommand);
      executor.setProgressListener(subCommand.getProgressListener());
      executor.parse(subCommand);
      subCommand.setContext(executor.getContext());

      if (!(executor instanceof Iterable<?>))
        throw new OCommandSQLParsingException("Sub-query cannot be iterated because doesn't implement the Iterable interface: "
            + subCommand);

      targetRecords = (Iterable<? extends OIdentifiable>) executor;
      final OCommandContext subContext = subCommand.getContext();

      // MERGE THE CONTEXTS
      if (context != null)
        context.merge(subContext);
      else
        context = subContext;
    } else if (c == OStringSerializerHelper.COLLECTION_BEGIN) {
      // COLLECTION OF RIDS
      final List<String> rids = new ArrayList<String>();
      currentPos = OStringSerializerHelper.getCollection(text, currentPos, rids);

      targetRecords = new ArrayList<OIdentifiable>();
      for (String rid : rids)
        ((List<OIdentifiable>) targetRecords).add(new ORecordId(rid));

      if (currentPos > -1)
        currentPos++;
    } else {
      String subjectName;
      String alias;
      String subjectToMatch;
      int newPos;

      final StringBuilder word = new StringBuilder();
      currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

      while (currentPos > -1 && (targetClasses == null && targetClusters == null && targetIndex == null)) {
        subjectName = word.toString();

        newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
        if (newPos > -1 && word.toString().equals("AS")) {
          currentPos = newPos;

          newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
          if (newPos == -1)
            throw new OQueryParsingException("No alias found. Example: SELECT FROM Customer AS c", text, currentPos);

          currentPos = newPos;

          alias = word.toString();

        } else
          alias = subjectName;

        subjectToMatch = subjectName;
        if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
          // REGISTER AS CLUSTER
          if (targetClusters == null)
            targetClusters = new HashMap<String, String>();
          targetClusters.put(subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length()), alias);

        } else if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {
          // REGISTER AS INDEX
          targetIndex = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());
        } else {
          if (subjectToMatch.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
            // REGISTER AS CLASS
            subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

          // REGISTER AS CLASS
          if (targetClasses == null)
            targetClasses = new HashMap<OClass, String>();

          final OClass cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(subjectName);
          if (cls == null)
            throw new OCommandExecutionException("Class '" + subjectName + "' was not found in current database");

          targetClasses.put(cls, alias);
        }
      }
    }

    return currentPos > -1;
  }

  public Map<String, String> getTargetClusters() {
    return targetClusters;
  }

  public Map<OClass, String> getTargetClasses() {
    return targetClasses;
  }

  public Iterable<? extends OIdentifiable> getTargetRecords() {
    return targetRecords;
  }

  public String getTargetIndex() {
    return targetIndex;
  }

  public OSQLFilterCondition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    if (rootCondition != null)
      return "Parsed: " + rootCondition.toString();
    return "Unparsed: " + text;
  }

}
