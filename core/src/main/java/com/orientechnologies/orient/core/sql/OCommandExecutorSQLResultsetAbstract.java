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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClusters;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

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
public abstract class OCommandExecutorSQLResultsetAbstract extends OCommandExecutorSQLAbstract implements Iterator<OIdentifiable>,
    Iterable<OIdentifiable> {
  protected static final String                    KEYWORD_FROM_2FIND = " " + KEYWORD_FROM + " ";
  protected static final String                    KEYWORD_LET_2FIND  = " " + KEYWORD_LET + " ";

  protected OSQLAsynchQuery<ORecordSchemaAware<?>> request;
  protected OSQLTarget                             parsedTarget;
  protected OSQLFilter                             compiledFilter;
  protected Map<String, Object>                    let                = null;
  protected Iterator<? extends OIdentifiable>      target;
  protected Iterable<OIdentifiable>                tempResult;
  protected int                                    resultCount;
  protected int                                    skip               = 0;

  /**
   * Compile the filter conditions only the first time.
   */
  public OCommandExecutorSQLResultsetAbstract parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    init(textRequest);

    if (iRequest instanceof OSQLSynchQuery) {
      request = (OSQLSynchQuery<ORecordSchemaAware<?>>) iRequest;
    } else if (iRequest instanceof OSQLAsynchQuery)
      request = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iRequest;
    else {
      // BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
      request = new OSQLSynchQuery<ORecordSchemaAware<?>>(textRequest.getText());
      if (textRequest.getResultListener() != null)
        request.setResultListener(textRequest.getResultListener());
    }
    return this;
  }

  @Override
  public boolean isReplicated() {
    return true;
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  /**
   * Assign the right TARGET if found.
   * 
   * @param iArgs
   *          Parameters to bind
   * @return true if the target has been recognized, otherwise false
   */
  protected boolean assignTarget(final Map<Object, Object> iArgs) {
    parameters = iArgs;
    if (parsedTarget == null)
      return true;

    if (iArgs != null && iArgs.size() > 0 && compiledFilter != null)
      compiledFilter.bindParameters(iArgs);

    if (target == null)
      if (parsedTarget.getTargetClasses() != null)
        searchInClasses();
      else if (parsedTarget.getTargetClusters() != null)
        searchInClusters();
      else if (parsedTarget.getTargetRecords() != null)
        target = parsedTarget.getTargetRecords().iterator();
      else if (parsedTarget.getTargetVariable() != null) {
        final Object var = getContext().getVariable(parsedTarget.getTargetVariable());
        if (var == null) {
          target = Collections.EMPTY_LIST.iterator();
          return true;
        } else if (var instanceof OIdentifiable) {
          final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>();
          list.add((OIdentifiable) var);
          target = list.iterator();
        } else if (var instanceof Iterable<?>)
          target = ((Iterable<? extends OIdentifiable>) var).iterator();
      } else
        return false;

    return true;
  }

  protected Object getResult() {
    if (tempResult != null) {
      for (Object d : tempResult)
        if (d != null) {
          if (!(d instanceof OIdentifiable))
            // NON-DOCUMENT AS RESULT, COMES FROM EXPAND? CREATE A DOCUMENT AT THE FLY
            d = new ODocument().field("value", d);

          request.getResultListener().result(d);
        }
    }

    if (request instanceof OSQLSynchQuery)
      return ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

    return null;
  }

  protected boolean handleResult(final OIdentifiable iRecord, boolean iCloneIt) {
    if (iRecord != null) {
      resultCount++;

      OIdentifiable recordCopy = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();

      if (recordCopy != null)
        // CALL THE LISTENER NOW
        if (request.getResultListener() != null)
          request.getResultListener().result(recordCopy);

      if (limit > -1 && resultCount >= limit)
        // BREAK THE EXECUTION
        return false;
    }
    return true;
  }

  protected void parseLet() {
    let = new LinkedHashMap<String, Object>();

    boolean stop = false;
    while (!stop) {
      // PARSE THE KEY
      parserNextWord(false);
      final String letName = parserGetLastWord();

      parserOptionalKeyword("=");

      parserNextWord(false, " =><,\r\n");

      // PARSE THE VALUE
      String letValueAsString = parserGetLastWord();
      final Object letValue;

      // TRY TO PARSE AS FUNCTION
      final Object func = OSQLHelper.getFunction(parsedTarget, letValueAsString);
      if (func != null)
        letValue = func;
      else if (letValueAsString.startsWith("(")) {
        letValue = new OSQLSynchQuery<Object>(letValueAsString.substring(1, letValueAsString.length() - 1));
      } else
        letValue = letValueAsString;

      let.put(letName, letValue);
      stop = parserGetLastSeparator() == ' ';
    }
  }

  /**
   * Parses the limit keyword if found.
   * 
   * @param w
   * 
   * @return
   * @return the limit found as integer, or -1 if no limit is found. -1 means no limits.
   * @throws OCommandSQLParsingException
   *           if no valid limit has been found
   */
  protected int parseLimit(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_LIMIT))
      return -1;

    parserNextWord(true);
    final String word = parserGetLastWord();

    try {
      limit = Integer.parseInt(word);
    } catch (Exception e) {
      throwParsingException("Invalid LIMIT value setted to '" + word + "' but it should be a valid integer. Example: LIMIT 10");
    }

    if (limit == 0)
      throwParsingException("Invalid LIMIT value setted to ZERO. Use -1 to ignore the limit or use a positive number. Example: LIMIT 10");

    return limit;
  }

  /**
   * Parses the skip keyword if found.
   * 
   * @param w
   * 
   * @return
   * @return the skip found as integer, or -1 if no skip is found. -1 means no skip.
   * @throws OCommandSQLParsingException
   *           if no valid skip has been found
   */
  protected int parseSkip(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_SKIP))
      return -1;

    parserNextWord(true);
    final String word = parserGetLastWord();

    try {
      skip = Integer.parseInt(word);

    } catch (Exception e) {
      throwParsingException("Invalid SKIP value setted to '" + word
          + "' but it should be a valid positive integer. Example: SKIP 10");
    }

    if (skip < 0)
      throwParsingException("Invalid SKIP value setted to the negative number '" + word
          + "'. Only positive numbers are valid. Example: SKIP 10");

    return skip;
  }

  protected boolean filter(final ORecordInternal<?> iRecord) {
    context.setVariable("current", iRecord);

    if (iRecord instanceof ORecordSchemaAware<?>) {
      // CHECK THE TARGET CLASS
      final ORecordSchemaAware<?> recordSchemaAware = (ORecordSchemaAware<?>) iRecord;
      Map<OClass, String> targetClasses = parsedTarget.getTargetClasses();
      // check only classes that specified in query will go to result set
      if ((targetClasses != null) && (!targetClasses.isEmpty())) {
        for (OClass targetClass : targetClasses.keySet()) {
          if (!targetClass.isSuperClassOf(recordSchemaAware.getSchemaClass()))
            return false;
        }
        context.updateMetric("documentAnalyzedCompatibleClass", +1);
      }
    }

    return evaluateRecord(iRecord);
  }

  protected boolean evaluateRecord(final ORecord<?> iRecord) {
    assignLetClauses(iRecord);
    if (compiledFilter == null)
      return true;
    return (Boolean) compiledFilter.evaluate(iRecord, null, context);
  }

  protected void assignLetClauses(final ORecord<?> iRecord) {
    if (let != null && !let.isEmpty()) {
      // BIND CONTEXT VARIABLES
      for (Entry<String, Object> entry : let.entrySet()) {
        String varName = entry.getKey();
        if (varName.startsWith("$"))
          varName = varName.substring(1);

        final Object letValue = entry.getValue();

        Object varValue;
        if (letValue instanceof OSQLSynchQuery<?>) {
          final OSQLSynchQuery<Object> subQuery = (OSQLSynchQuery<Object>) letValue;
          subQuery.reset();
          subQuery.resetPagination();
          subQuery.getContext().setParent(context);
          subQuery.getContext().setVariable("current", iRecord);
          varValue = ODatabaseRecordThreadLocal.INSTANCE.get().query(subQuery);
        } else if (letValue instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime f = (OSQLFunctionRuntime) letValue;
          if (f.getFunction().aggregateResults()) {
            f.execute(iRecord, null, context);
            varValue = f.getFunction().getResult();
          } else
            varValue = f.execute(iRecord, null, context);
        } else
          varValue = ODocumentHelper.getFieldValue(iRecord, ((String) letValue).trim(), context);

        context.setVariable(varName, varValue);
      }
    }
  }

  protected void searchInClasses() {
    final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();

    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, cls.getName().toLowerCase());

    // NO INDEXES: SCAN THE ENTIRE CLUSTER
    final ORID[] range = getRange();
    target = new ORecordIteratorClass<ORecordInternal<?>>(database, (ODatabaseRecordAbstract) database, cls.getName(), true,
        request.isUseCache(), false).setRange(range[0], range[1]);
  }

  protected void searchInClusters() {
    final ODatabaseRecord database = getDatabase();

    final Set<Integer> clusterIds = new HashSet<Integer>();
    for (String clusterName : parsedTarget.getTargetClusters().keySet()) {
      if (clusterName == null || clusterName.length() == 0)
        throw new OCommandExecutionException("No cluster or schema class selected in query");

      database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, clusterName.toLowerCase());

      if (Character.isDigit(clusterName.charAt(0))) {
        // GET THE CLUSTER NUMBER
        for (int clusterId : OStringSerializerHelper.splitIntArray(clusterName)) {
          if (clusterId == -1)
            throw new OCommandExecutionException("Cluster '" + clusterName + "' not found");

          clusterIds.add(clusterId);
        }
      } else {
        // GET THE CLUSTER NUMBER BY THE CLASS NAME
        final int clusterId = database.getClusterIdByName(clusterName.toLowerCase());
        if (clusterId == -1)
          throw new OCommandExecutionException("Cluster '" + clusterName + "' not found");

        clusterIds.add(clusterId);
      }
    }

    // CREATE CLUSTER AS ARRAY OF INT
    final int[] clIds = new int[clusterIds.size()];
    int i = 0;
    for (int c : clusterIds)
      clIds[i++] = c;

    final ORID[] range = getRange();

    target = new ORecordIteratorClusters<ORecordInternal<?>>(database, database, clIds, request.isUseCache(), false).setRange(
        range[0], range[1]);
  }

  protected void applyLimitAndSkip() {
    if (tempResult != null && (limit > 0 || skip > 0)) {
      final List<OIdentifiable> newList = new ArrayList<OIdentifiable>();

      // APPLY LIMIT
      if (tempResult instanceof List<?>) {
        final List<OIdentifiable> t = (List<OIdentifiable>) tempResult;
        final int start = Math.min(skip, t.size());
        final int tot = Math.min(limit + start, t.size());
        for (int i = start; i < tot; ++i)
          newList.add(t.get(i));

        t.clear();
        tempResult = newList;
      }
    }
  }

  /**
   * Optimizes the condition tree.
   * 
   * @return
   */
  protected void optimize() {
    if (compiledFilter != null)
      optimizeBranch(null, compiledFilter.getRootCondition());
  }

  /**
   * Check function arguments and pre calculate it if possible
   * 
   * @param function
   * @return optimized function, same function if no change
   */
  protected Object optimizeFunction(OSQLFunctionRuntime function) {
    // boolean precalculate = true;
    // for (int i = 0; i < function.configuredParameters.length; ++i) {
    // if (function.configuredParameters[i] instanceof OSQLFilterItemField) {
    // precalculate = false;
    // } else if (function.configuredParameters[i] instanceof OSQLFunctionRuntime) {
    // final Object res = optimizeFunction((OSQLFunctionRuntime) function.configuredParameters[i]);
    // function.configuredParameters[i] = res;
    // if (res instanceof OSQLFunctionRuntime || res instanceof OSQLFilterItemField) {
    // // function might have been optimized but result is still not static
    // precalculate = false;
    // }
    // }
    // }
    //
    // if (precalculate) {
    // // all fields are static, we can calculate it only once.
    // return function.execute(null, null, null); // we can pass nulls here, they wont be used
    // } else {
    return function;
    // }
  }

  protected void optimizeBranch(final OSQLFilterCondition iParentCondition, OSQLFilterCondition iCondition) {
    if (iCondition == null)
      return;

    Object left = iCondition.getLeft();

    if (left instanceof OSQLFilterCondition) {
      // ANALYSE LEFT RECURSIVELY
      optimizeBranch(iCondition, (OSQLFilterCondition) left);
    } else if (left instanceof OSQLFunctionRuntime) {
      left = optimizeFunction((OSQLFunctionRuntime) left);
      iCondition.setLeft(left);
    }

    Object right = iCondition.getRight();

    if (right instanceof OSQLFilterCondition) {
      // ANALYSE RIGHT RECURSIVELY
      optimizeBranch(iCondition, (OSQLFilterCondition) right);
    } else if (right instanceof OSQLFunctionRuntime) {
      right = optimizeFunction((OSQLFunctionRuntime) right);
      iCondition.setRight(right);
    }

    final OQueryOperator oper = iCondition.getOperator();

    Object result = null;

    if (left instanceof OSQLFilterItemField && right instanceof OSQLFilterItemField) {
      if (((OSQLFilterItemField) left).getRoot().equals(((OSQLFilterItemField) right).getRoot())) {
        if (oper instanceof OQueryOperatorEquals)
          result = Boolean.TRUE;
        else if (oper instanceof OQueryOperatorNotEquals)
          result = Boolean.FALSE;
      }
    }

    if (result != null) {
      if (iParentCondition != null)
        if (iCondition == iParentCondition.getLeft())
          // REPLACE LEFT
          iCondition.setLeft(result);
        else
          // REPLACE RIGHT
          iCondition.setRight(result);
      else {
        // REPLACE ROOT CONDITION
        if (result instanceof Boolean && ((Boolean) result))
          compiledFilter.setRootCondition(null);
      }
    }
  }

  protected ORID[] getRange() {
    final ORID beginRange;
    final ORID endRange;

    final OSQLFilterCondition rootCondition = compiledFilter == null ? null : compiledFilter.getRootCondition();
    if (compiledFilter == null || rootCondition == null) {
      if (request instanceof OSQLSynchQuery)
        beginRange = ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getNextPageRID();
      else
        beginRange = null;
      endRange = null;
    } else {
      final ORID conditionBeginRange = rootCondition.getBeginRidRange();
      final ORID conditionEndRange = rootCondition.getEndRidRange();
      final ORID nextPageRid;

      if (request instanceof OSQLSynchQuery)
        nextPageRid = ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getNextPageRID();
      else
        nextPageRid = null;

      if (conditionBeginRange != null && nextPageRid != null)
        beginRange = conditionBeginRange.compareTo(nextPageRid) > 0 ? conditionBeginRange : nextPageRid;
      else if (conditionBeginRange != null)
        beginRange = conditionBeginRange;
      else
        beginRange = nextPageRid;

      endRange = conditionEndRange;
    }

    return new ORID[] { beginRange, endRange };
  }
}
