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
package com.orientechnologies.orient.core.sql.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
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
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OCommandSQLResultset;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Parsed query. It's built once a query is parsed.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilter extends OSQLPredicate implements OCommandPredicate {
  protected String                            targetVariable;
  protected Iterable<? extends OIdentifiable> targetRecords;
  protected Map<String, String>               targetClusters;
  protected Map<OClass, String>               targetClasses;
  protected String                            targetIndex;

  public OSQLFilter(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    super();
    context = iContext;
    parserText = iText;
    parserTextUpperCase = iText.toUpperCase();

    try {
      if (extractTargets()) {
        // IF WHERE EXISTS EXTRACT CONDITIONS

        if (parserOptionalKeyword(OCommandExecutorSQLAbstract.KEYWORD_LET, OCommandExecutorSQLAbstract.KEYWORD_LIMIT,
            OCommandExecutorSQLSelect.KEYWORD_ORDER, OCommandExecutorSQLSelect.KEYWORD_SKIP, iFilterKeyword,
            OCommandExecutorSQLSelect.KEYWORD_WHERE)) { // TODO Remove the additional management of WHERE for TRAVERSE after a while

          boolean continueParsing = true;
          if (parserGetLastWord().equals(OCommandExecutorSQLAbstract.KEYWORD_LET)) {
            extractLet(iFilterKeyword);
            continueParsing = parserOptionalKeyword(OCommandExecutorSQLAbstract.KEYWORD_LIMIT,
                OCommandExecutorSQLSelect.KEYWORD_ORDER, OCommandExecutorSQLSelect.KEYWORD_SKIP, iFilterKeyword,
                OCommandExecutorSQLSelect.KEYWORD_WHERE);// TODO Remove the additional management of WHERE for TRAVERSE after a
                                                         // while
          }

          if (continueParsing) {

            if (parserGetLastWord().equals(iFilterKeyword) || parserGetLastWord().equals(OCommandExecutorSQLSelect.KEYWORD_WHERE)) {
              if (!iFilterKeyword.equals(OCommandExecutorSQLSelect.KEYWORD_WHERE)
                  && parserGetLastWord().equals(OCommandExecutorSQLSelect.KEYWORD_WHERE))
                // TODO Remove the additional management of WHERE for TRAVERSE after a while
                warnDeprecatedWhere();

              final int lastPos = parserGetCurrentPosition();
              final String lastText = parserText;
              final String lastTextUpperCase = parserTextUpperCase;

              text(parserText.substring(lastPos));

              parserText = lastText;
              parserTextUpperCase = lastTextUpperCase;
              parserMoveCurrentPosition(lastPos);

            } else
              parserGoBack();
          } else
            parserGoBack();
        }
      }
    } catch (OQueryParsingException e) {
      if (e.getText() == null)
        // QUERY EXCEPTION BUT WITHOUT TEXT: NEST IT
        throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), e);

      throw e;
    } catch (Throwable t) {
      throw new OQueryParsingException("Error on parsing query", parserText, parserGetCurrentPosition(), t);
    }
  }

  protected void warnDeprecatedWhere() {
    OLogManager
        .instance()
        .warn(
            this,
            "Keyword WHERE in traverse has been replaced by WHILE. Please change your query to support WHILE instead of WHERE because now it's only deprecated, but in future it will be removed the back-ward compatibility.");
  }

  public boolean evaluate(final ORecord<?> iRecord, final OCommandContext iContext) {
    if (let != null && !let.isEmpty()) {
      // BIND CONTEXT VARIABLES
      for (Entry<String, String> entry : let.entrySet()) {
        String varName = entry.getKey();
        if (varName.startsWith("$"))
          varName = varName.substring(1);

        Object varValue;
        final String varValueAsString = entry.getValue().trim();
        if (varValueAsString.startsWith("(")) {
          final OSQLSynchQuery<Object> subQuery = new OSQLSynchQuery<Object>(varValueAsString.substring(1,
              varValueAsString.length() - 1));
          subQuery.setContext(context);
          varValue = ODatabaseRecordThreadLocal.INSTANCE.get().query(subQuery);
        } else
          varValue = ODocumentHelper.getFieldValue(iRecord, varValueAsString);

        context.setVariable(varName, varValue);
      }
    }

    if (rootCondition == null)
      return true;

    return (Boolean) rootCondition.evaluate(iRecord, iContext);
  }

  @SuppressWarnings("unchecked")
  private boolean extractTargets() {
    parserSkipWhiteSpaces();

    if (parserIsEnded())
      throw new OQueryParsingException("No query target found", parserText, 0);

    final char c = parserGetCurrentChar();

    if (c == '$') {
      targetVariable = parserRequiredWord(true, "No valid target");
      targetVariable = targetVariable.substring(1);
    } else if (c == '#' || Character.isDigit(c)) {
      // UNIQUE RID
      targetRecords = new ArrayList<OIdentifiable>();
      ((List<OIdentifiable>) targetRecords).add(new ORecordId(parserRequiredWord(true, "No valid RID")));

    } else if (c == '(') {
      // SUB QUERY
      final StringBuilder subText = new StringBuilder();
      parserSetCurrentPosition(OStringSerializerHelper.getEmbedded(parserText, parserGetCurrentPosition(), -1, subText) + 1);
      final OCommandSQL subCommand = new OCommandSQLResultset(subText.toString());

      final OCommandExecutor executor = OCommandManager.instance().getExecutor(subCommand);
      executor.setProgressListener(subCommand.getProgressListener());
      executor.parse(subCommand);
      executor.getContext().merge(context);

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
      parserSetCurrentPosition(OStringSerializerHelper.getCollection(parserText, parserGetCurrentPosition(), rids));

      targetRecords = new ArrayList<OIdentifiable>();
      for (String rid : rids)
        ((List<OIdentifiable>) targetRecords).add(new ORecordId(rid));

      parserMoveCurrentPosition(1);
    } else {

      while (!parserIsEnded() && (targetClasses == null && targetClusters == null && targetIndex == null)) {
        String subjectName = parserRequiredWord(true, "Target not found");

        final String alias;
        if (subjectName.equals("AS"))
          alias = parserRequiredWord(true, "Alias not found");
        else
          alias = subjectName;

        final String subjectToMatch = subjectName;
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

    return !parserIsEnded();
  }

  protected void extractLet(final String iFilterKeyword) {
    let = new LinkedHashMap<String, String>();

    boolean stop = false;
    while (!stop) {
      parserNextWord(false);
      final String letName = parserGetLastWord();

      parserOptionalKeyword("=");

      parserNextWord(false, " =><,\r\n");
      final String letValue = parserGetLastWord();

      let.put(letName, letValue);
      stop = parserGetLastSeparator() == ' ';
    }
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
    return "Unparsed: " + parserText;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

}
