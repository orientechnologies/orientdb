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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.collection.OSortedMultiIterator;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.*;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.operator.*;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import com.orientechnologies.orient.core.sql.parser.OOrderByItem;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and builds the meta information needed by the execute().
 * If the query contains the ORDER BY clause, the results are temporary collected internally, then ordered and finally returned all
 * together to the listener.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSelect extends OCommandExecutorSQLResultsetAbstract {
  public static final String          KEYWORD_SELECT       = "SELECT";
  public static final String          KEYWORD_ASC          = "ASC";
  public static final String          KEYWORD_DESC         = "DESC";
  public static final String          KEYWORD_ORDER        = "ORDER";
  public static final String          KEYWORD_BY           = "BY";
  public static final String          KEYWORD_GROUP        = "GROUP";
  public static final String          KEYWORD_UNWIND       = "UNWIND";
  public static final String          KEYWORD_FETCHPLAN    = "FETCHPLAN";
  public static final String          KEYWORD_NOCACHE      = "NOCACHE";
  private static final String         KEYWORD_AS           = "AS";
  private static final String         KEYWORD_PARALLEL     = "PARALLEL";
  private final OOrderByOptimizer     orderByOptimizer     = new OOrderByOptimizer();
  private final OMetricRecorder       metricRecorder       = new OMetricRecorder();
  private final OFilterOptimizer      filterOptimizer      = new OFilterOptimizer();
  private final OFilterAnalyzer       filterAnalyzer       = new OFilterAnalyzer();
  private Map<String, String>         projectionDefinition = null;
  // THIS HAS BEEN KEPT FOR COMPATIBILITY; BUT IT'S USED THE PROJECTIONS IN GROUPED-RESULTS
  private Map<String, Object>         projections          = null;
  private List<OPair<String, String>> orderedFields        = new ArrayList<OPair<String, String>>();
  private List<String>                groupByFields;
  private Map<Object, ORuntimeResult> groupedResult;
  private List<String>                unwindFields;
  private Object                      expandTarget;
  private int                         fetchLimit           = -1;
  private OIdentifiable               lastRecord;
  private String                      fetchPlan;
  private volatile boolean            executing;
  private boolean                     fullySortedByIndex   = false;
  private LOCKING_STRATEGY            lockingStrategy      = LOCKING_STRATEGY.DEFAULT;
  private boolean                     parallel             = false;
  private Lock                        parallelLock         = new ReentrantLock();
  private Set<ORID>                   uniqueResult;
  private boolean                     noCache              = false;
  private int                         tipLimitThreshold    = OGlobalConfiguration.QUERY_LIMIT_THRESHOLD_TIP.getValueAsInteger();
  private String                      NULL_VALUE           = "null";

  public OCommandExecutorSQLSelect() {
  }

  private static final class IndexUsageLog {
    OIndex<?>        index;
    List<Object>     keyParams;
    OIndexDefinition indexDefinition;

    IndexUsageLog(OIndex<?> index, List<Object> keyParams, OIndexDefinition indexDefinition) {
      this.index = index;
      this.keyParams = keyParams;
      this.indexDefinition = indexDefinition;
    }
  }

  private final class IndexComparator implements Comparator<OIndex<?>> {
    public int compare(final OIndex<?> indexOne, final OIndex<?> indexTwo) {
      final OIndexDefinition definitionOne = indexOne.getDefinition();
      final OIndexDefinition definitionTwo = indexTwo.getDefinition();

      final int firstParamCount = definitionOne.getParamCount();
      final int secondParamCount = definitionTwo.getParamCount();

      final int result = firstParamCount - secondParamCount;

      if (result == 0 && !orderedFields.isEmpty()) {
        if (!(indexOne instanceof OChainedIndexProxy)
            && orderByOptimizer.canBeUsedByOrderBy(indexOne, OCommandExecutorSQLSelect.this.orderedFields)) {
          return 1;
        }

        if (!(indexTwo instanceof OChainedIndexProxy)
            && orderByOptimizer.canBeUsedByOrderBy(indexTwo, OCommandExecutorSQLSelect.this.orderedFields)) {
          return -1;
        }
      }

      return result;
    }
  }

  private static Object getIndexKey(final OIndexDefinition indexDefinition, Object value, OCommandContext context) {
    if (indexDefinition instanceof OCompositeIndexDefinition || indexDefinition.getParamCount() > 1) {
      if (value instanceof List) {
        final List<?> values = (List<?>) value;
        List<Object> keyParams = new ArrayList<Object>(values.size());

        for (Object o : values) {
          keyParams.add(OSQLHelper.getValue(o, null, context));
        }
        return indexDefinition.createValue(keyParams);
      } else {
        value = OSQLHelper.getValue(value);
        if (value instanceof OCompositeKey) {
          return value;
        } else {
          return indexDefinition.createValue(value);
        }
      }
    } else {
      return indexDefinition.createValue(OSQLHelper.getValue(value));
    }
  }

  @Override
  protected boolean isUseCache() {
    return !noCache && request.isUseCache();
  }

  private static ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
    final ODocument doc = new ODocument().setOrdered(true);
    doc.field("key", iKey);
    doc.field("rid", iValue);
    ORecordInternal.unsetDirty(doc);
    return doc;
  }

  /**
   * Compile the filter conditions only the first time.
   */
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;
    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      // System.out.println("NEW PARSER FROM: " + queryText);
      queryText = preParse(queryText, iRequest);
      // System.out.println("NEW PARSER TO: " + queryText);
      textRequest.setText(queryText);

      super.parse(iRequest);

      initContext();

      final int pos = parseProjections();
      if (pos == -1) {
        return this;
      }

      final int endPosition = parserText.length();

      parserNextWord(true);
      if (parserGetLastWord().equalsIgnoreCase(KEYWORD_FROM)) {
        // FROM
        parsedTarget = OSQLEngine.getInstance().parseTarget(parserText.substring(parserGetCurrentPosition(), endPosition),
            getContext(), KEYWORD_WHERE);
        parserSetCurrentPosition(
            parsedTarget.parserIsEnded() ? endPosition : parsedTarget.parserGetCurrentPosition() + parserGetCurrentPosition());
      } else {
        parserGoBack();
      }

      if (!parserIsEnded()) {
        parserSkipWhiteSpaces();

        while (!parserIsEnded()) {
          final String w = parserNextWord(true);

          if (!w.isEmpty()) {
            if (w.equals(KEYWORD_WHERE)) {
              compiledFilter = OSQLEngine.getInstance()
                  .parseCondition(parserText.substring(parserGetCurrentPosition(), endPosition), getContext(), KEYWORD_WHERE);
              optimize();
              parserSetCurrentPosition(compiledFilter.parserIsEnded() ? endPosition
                  : compiledFilter.parserGetCurrentPosition() + parserGetCurrentPosition());
            } else if (w.equals(KEYWORD_LET)) {
              parseLet();
            } else if (w.equals(KEYWORD_GROUP)) {
              parseGroupBy();
            } else if (w.equals(KEYWORD_ORDER)) {
              parseOrderBy();
            } else if (w.equals(KEYWORD_UNWIND)) {
              parseUnwind();
            } else if (w.equals(KEYWORD_LIMIT)) {
              parseLimit(w);
            } else if (w.equals(KEYWORD_SKIP) || w.equals(KEYWORD_OFFSET)) {
              parseSkip(w);
            } else if (w.equals(KEYWORD_FETCHPLAN)) {
              parseFetchplan(w);
            } else if (w.equals(KEYWORD_NOCACHE)) {
              parseNoCache(w);
            } else if (w.equals(KEYWORD_TIMEOUT)) {
              parseTimeout(w);
            } else if (w.equals(KEYWORD_LOCK)) {
              final String lock = parseLock();

              if (lock.equalsIgnoreCase("DEFAULT")) {
                lockingStrategy = LOCKING_STRATEGY.DEFAULT;
              } else if (lock.equals("NONE")) {
                lockingStrategy = LOCKING_STRATEGY.NONE;
              } else if (lock.equals("RECORD")) {
                lockingStrategy = LOCKING_STRATEGY.EXCLUSIVE_LOCK;
              }
            } else if (w.equals(KEYWORD_PARALLEL)) {
              parallel = parseParallel(w);
            } else {
              throwParsingException("Invalid keyword '" + w + "'");
            }
          }
        }
      }
      if (limit == 0 || limit < -1) {
        throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
      }
      validateQuery();
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  private void validateQuery() {
    if (this.let != null) {
      for (Object letValue : let.values()) {
        if (letValue instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime f = (OSQLFunctionRuntime) letValue;
          if (f.getFunction().aggregateResults() && this.groupByFields != null && this.groupByFields.size() > 0) {
            throwParsingException("Aggregate function cannot be used in LET clause together with GROUP BY");
          }
        }
      }
    }
  }

  /**
   * Determine clusters that are used in select operation
   *
   * @return set of involved cluster names
   */
  @Override
  public Set<String> getInvolvedClusters() {

    final Set<String> clusters = new HashSet<String>();

    if (parsedTarget != null) {
      final ODatabaseDocument db = getDatabase();

      if (parsedTarget.getTargetQuery() != null
          && parsedTarget.getTargetRecords() instanceof OCommandExecutorSQLResultsetDelegate) {
        // SUB-QUERY: EXECUTE IT LOCALLY
        // SUB QUERY, PROPAGATE THE CALL
        final Set<String> clIds = ((OCommandExecutorSQLResultsetDelegate) parsedTarget.getTargetRecords()).getInvolvedClusters();
        for (String c : clIds) {
          // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
          if (checkClusterAccess(db, c)) {
            clusters.add(c);
          }
        }

      } else if (parsedTarget.getTargetRecords() != null) {
        // SINGLE RECORDS: BROWSE ALL (COULD BE EXPENSIVE).
        for (OIdentifiable identifiable : parsedTarget.getTargetRecords()) {
          final String c = db.getClusterNameById(identifiable.getIdentity().getClusterId()).toLowerCase();
          // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
          if (checkClusterAccess(db, c)) {
            clusters.add(c);
          }
        }
      }

      if (parsedTarget.getTargetClasses() != null) {
        return getInvolvedClustersOfClasses(parsedTarget.getTargetClasses().values());
      }

      if (parsedTarget.getTargetClusters() != null) {
        return getInvolvedClustersOfClusters(parsedTarget.getTargetClusters().keySet());
      }

      if (parsedTarget.getTargetIndex() != null) {
        // EXTRACT THE CLASS NAME -> CLUSTERS FROM THE INDEX DEFINITION
        return getInvolvedClustersOfIndex(parsedTarget.getTargetIndex());
      }

    }
    return clusters;
  }

  /**
   * @return {@code ture} if any of the sql functions perform aggregation, {@code false} otherwise
   */
  public boolean isAnyFunctionAggregates() {
    if (projections != null) {
      for (Entry<String, Object> p : projections.entrySet()) {
        if (p.getValue() instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) p.getValue()).aggregateResults()) {
          return true;
        }
      }
    }
    return false;
  }

  public Iterator<OIdentifiable> iterator() {
    return iterator(null);
  }

  public Iterator<OIdentifiable> iterator(final Map<Object, Object> iArgs) {
    final Iterator<OIdentifiable> subIterator;
    if (target == null) {
      // GET THE RESULT
      executeSearch(iArgs);
      applyExpand();
      handleNoTarget();
      handleGroupBy();
      applyOrderBy();

      subIterator = new ArrayList<OIdentifiable>((List<OIdentifiable>) getResult()).iterator();
      lastRecord = null;
      tempResult = null;
      groupedResult = null;
    } else {
      subIterator = (Iterator<OIdentifiable>) target;
    }

    return subIterator;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    try {
      bindDefaultContextVariables();

      if (iArgs != null)
      // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
      {
        for (Entry<Object, Object> arg : iArgs.entrySet()) {
          context.setVariable(arg.getKey().toString(), arg.getValue());
        }
      }

      if (timeoutMs > 0) {
        getContext().beginExecution(timeoutMs, timeoutStrategy);
      }

      if (!optimizeExecution()) {
        fetchLimit = getQueryFetchLimit();

        executeSearch(iArgs);
        applyExpand();
        handleNoTarget();
        handleGroupBy();
        applyOrderBy();
        applyLimitAndSkip();
      }
      return getResult();
    } finally {
      if (request.getResultListener() != null) {
        request.getResultListener().end();
      }
    }
  }

  public Map<String, Object> getProjections() {
    return projections;
  }

  @Override
  public String getSyntax() {
    return "SELECT [<Projections>] FROM <Target> [LET <Assignment>*] [WHERE <Condition>*] [ORDER BY <Fields>* [ASC|DESC]*] [LIMIT <MaxRecords>] [TIMEOUT <TimeoutInMs>] [LOCK none|record] [NOCACHE]";
  }

  public String getFetchPlan() {
    return fetchPlan != null ? fetchPlan : request.getFetchPlan();
  }

  protected void executeSearch(final Map<Object, Object> iArgs) {
    assignTarget(iArgs);

    if (target == null) {
      if (let != null)
      // EXECUTE ONCE TO ASSIGN THE LET
      {
        assignLetClauses(lastRecord != null ? lastRecord.getRecord() : null);
      }

      // SEARCH WITHOUT USING TARGET (USUALLY WHEN LET/INDEXES ARE INVOLVED)
      return;
    }

    fetchFromTarget(target);
  }

  @Override
  protected boolean assignTarget(Map<Object, Object> iArgs) {
    if (!super.assignTarget(iArgs)) {
      if (parsedTarget.getTargetIndex() != null) {
        searchInIndex();
      } else {
        throw new OQueryParsingException(
            "No source found in query: specify class, cluster(s), index or single record(s). Use " + getSyntax());
      }
    }
    return true;
  }

  protected boolean executeSearchRecord(final OIdentifiable id) {
    if (Thread.interrupted()) {
      throw new OCommandExecutionException("The select execution has been interrupted");
    }

    if (!context.checkTimeout()) {
      return false;
    }

    final LOCKING_STRATEGY contextLockingStrategy = context.getVariable("$locking") != null
        ? (LOCKING_STRATEGY) context.getVariable("$locking") : null;

    final LOCKING_STRATEGY localLockingStrategy = contextLockingStrategy != null ? contextLockingStrategy : lockingStrategy;

    if (localLockingStrategy != null
        && !(localLockingStrategy == LOCKING_STRATEGY.DEFAULT || localLockingStrategy == LOCKING_STRATEGY.NONE
            || localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK || localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK))
      throw new IllegalStateException("Unsupported locking strategy " + localLockingStrategy);

    final ORecord record;
    if (!(id instanceof ORecord)) {
      record = getDatabase().load(id.getIdentity(), null, !isUseCache());
      if (id instanceof OContextualRecordId && ((OContextualRecordId) id).getContext() != null) {
        Map<String, Object> ridContext = ((OContextualRecordId) id).getContext();
        for (String key : ridContext.keySet()) {
          context.setVariable(key, ridContext.get(key));
        }
      }
    } else {
      record = (ORecord) id;
    }

    context.updateMetric("recordReads", +1);

    if (record == null || ORecordInternal.getRecordType(record) != ODocument.RECORD_TYPE)
    // SKIP IT
    {
      return true;
    }
    context.updateMetric("documentReads", +1);

    if (localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK) {
      record.lock(false);
      record.reload(null, true, false);
    } else if (localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK) {
      record.lock(true);
      record.reload(null, true, false);
    }

    try {
      context.setVariable("current", record);

      if (filter(record)) {
        if (!handleResult(record))
        // LIMIT REACHED
        {
          return false;
        }
      }
    } finally {
      if (localLockingStrategy != null && record.isLocked()) {
        // CONTEXT LOCK: lock must be released (no matter if filtered or not)
        if (localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK || localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK) {
          record.unlock();
        }
      }
    }
    return true;
  }

  /**
   * Handles the record in result.
   *
   * @param iRecord
   *          Record to handle
   * @return false if limit has been reached, otherwise true
   */
  protected boolean handleResult(final OIdentifiable iRecord) {
    if (parallel)
      // LOCK FOR PARALLEL EXECUTION. THIS PREVENT CONCURRENT ISSUES
      parallelLock.lock();

    try {
      if ((orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort()) && skip > 0 && this.unwindFields == null) {
        lastRecord = null;
        skip--;
        return true;
      }

      lastRecord = iRecord;

      resultCount++;

      if (!addResult(lastRecord)) {
        return false;
      }

      return !((orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort()) && !isAnyFunctionAggregates()
          && (groupByFields == null || groupByFields.isEmpty()) && fetchLimit > -1 && resultCount >= fetchLimit);
    } finally {
      if (parallel)
        // UNLOCK PARALLEL EXECUTION
        parallelLock.unlock();
    }
  }

  /**
   * Returns the temporary RID counter assuring it's unique per query tree.
   * 
   * @return Serial as integer
   */
  protected int getTemporaryRIDCounter() {
    final OCommandExecutorSQLSelect parentQuery = (OCommandExecutorSQLSelect) context.getVariable("parentQuery");
    return parentQuery != null && parentQuery != this ? parentQuery.getTemporaryRIDCounter() : serialTempRID++;
  }

  protected boolean addResult(OIdentifiable iRecord) {
    if (iRecord == null)
      return true;

    if (projections != null || groupByFields != null && !groupByFields.isEmpty()) {
      if (groupedResult == null) {
        // APPLY PROJECTIONS IN LINE
        iRecord = ORuntimeResult.getProjectionResult(getTemporaryRIDCounter(), projections, context, iRecord);
        if (iRecord == null)
          return true;

      } else {
        // AGGREGATION/GROUP BY
        Object fieldValue = null;
        if (groupByFields != null && !groupByFields.isEmpty()) {
          if (groupByFields.size() > 1) {
            // MULTI-FIELD GROUP BY
            final ODocument doc = iRecord.getRecord();
            final Object[] fields = new Object[groupByFields.size()];
            for (int i = 0; i < groupByFields.size(); ++i) {
              final String field = groupByFields.get(i);
              if (field.startsWith("$"))
                fields[i] = context.getVariable(field);
              else
                fields[i] = doc.field(field);

            }
            fieldValue = fields;
          } else {
            final String field = groupByFields.get(0);
            if (field != null) {
              if (field.startsWith("$"))
                fieldValue = context.getVariable(field);
              else
                fieldValue = ((ODocument) iRecord.getRecord()).field(field);
            }
          }
        }

        getProjectionGroup(fieldValue).applyRecord(iRecord);
        return true;
      }
    }

    if (tipLimitThreshold > 0 && resultCount > tipLimitThreshold) {
      reportTip(String.format(
          "Query '%s' returned a result set with more than %d records. Reduce it to improve performance and reduce RAM used",
          parserText, tipLimitThreshold));
      tipLimitThreshold = 0;
    }

    List<OIdentifiable> allResults = new ArrayList<OIdentifiable>();
    if (unwindFields != null) {
      Collection<OIdentifiable> partial = unwind(iRecord, this.unwindFields);

      for (OIdentifiable item : partial) {
        allResults.add(item);
      }
    } else {
      allResults.add(iRecord);
    }
    boolean result = true;
    if ((fullySortedByIndex || orderedFields.isEmpty()) && expandTarget == null && unwindFields == null) {
      // SEND THE RESULT INLINE
      if (request.getResultListener() != null)
        for (OIdentifiable iRes : allResults) {
          result = pushResult(iRes);
        }
    } else {

      // COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
      if (tempResult == null)
        tempResult = new ArrayList<OIdentifiable>();

      for (OIdentifiable iRes : allResults) {
        ((Collection<OIdentifiable>) tempResult).add(iRes);
      }
    }

    return result;
  }

  private Collection<OIdentifiable> unwind(OIdentifiable iRecord, List<String> unwindFields) {
    List<OIdentifiable> result = new ArrayList<OIdentifiable>();
    ODocument doc;
    if (iRecord instanceof ODocument) {
      doc = (ODocument) iRecord;
    } else {
      doc = iRecord.getRecord();
    }
    if (unwindFields.size() == 0) {
      ORecordInternal.setIdentity(doc, new ORecordId(-2, getTemporaryRIDCounter()));
      result.add(doc);
    } else {
      String firstField = unwindFields.get(0);
      List<String> nextFields = unwindFields.subList(1, unwindFields.size());

      Object fieldValue = doc.field(firstField);
      if (fieldValue == null || !(fieldValue instanceof Iterable) || fieldValue instanceof ODocument) {
        result.addAll(unwind(doc, nextFields));
      } else {
        for (Object o : (Iterable) fieldValue) {
          ODocument unwindedDoc = new ODocument();
          doc.copyTo(unwindedDoc);
          unwindedDoc.field(firstField, o);
          result.addAll(unwind(unwindedDoc, nextFields));
        }
      }
    }
    return result;
  }

  /**
   * Report the tip to the profiler and collect it in context to be reported by tools like Studio
   * 
   * @param iMessage
   */
  protected void reportTip(final String iMessage) {
    Orient.instance().getProfiler().reportTip(iMessage);
    List<String> tips = (List<String>) context.getVariable("tips");
    if (tips == null) {
      tips = new ArrayList<String>(3);
      context.setVariable("tips", tips);
    }
    tips.add(iMessage);
  }

  protected ORuntimeResult getProjectionGroup(final Object fieldValue) {
    final long projectionElapsed = (Long) context.getVariable("projectionElapsed", 0l);
    final long begin = System.currentTimeMillis();
    try {

      Object key;
      if (groupedResult == null)
        groupedResult = new LinkedHashMap<Object, ORuntimeResult>();

      if (fieldValue != null) {
        if (fieldValue.getClass().isArray()) {
          // LOOK IT BY HASH (FASTER THAN COMPARE EACH SINGLE VALUE)
          final Object[] array = (Object[]) fieldValue;

          final StringBuilder keyArray = new StringBuilder();
          for (Object o : array) {
            if (keyArray.length() > 0) {
              keyArray.append(",");
            }
            if (o != null) {
              keyArray.append(o instanceof OIdentifiable ? ((OIdentifiable) o).getIdentity().toString() : o.toString());
            } else {
              keyArray.append(NULL_VALUE);
            }
          }

          key = keyArray.toString();
        } else {
          // LOOKUP FOR THE FIELD
          key = fieldValue;
        }
      } else
        // USE NULL_VALUE THEN REPLACE WITH REAL NULL
        key = NULL_VALUE;

      ORuntimeResult group = groupedResult.get(key);
      if (group == null) {
        group = new ORuntimeResult(fieldValue, createProjectionFromDefinition(), getTemporaryRIDCounter(), context);
        groupedResult.put(key, group);
      }
      return group;
    } finally {
      context.setVariable("projectionElapsed", projectionElapsed + (System.currentTimeMillis() - begin));
    }
  }

  protected void parseGroupBy() {
    parserRequiredKeyword(KEYWORD_BY);

    groupByFields = new ArrayList<String>();
    while (!parserIsEnded() && (groupByFields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");
      groupByFields.add(fieldName);
      parserSkipWhiteSpaces();
    }

    if (groupByFields.size() == 0) {
      throwParsingException("Group by field set was missed. Example: GROUP BY name, salary");
    }

    // AGGREGATE IT
    groupedResult = new LinkedHashMap<Object, ORuntimeResult>();
  }

  protected void parseUnwind() {
    unwindFields = new ArrayList<String>();
    while (!parserIsEnded() && (unwindFields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");
      unwindFields.add(fieldName);
      parserSkipWhiteSpaces();
    }

    if (unwindFields.size() == 0) {
      throwParsingException("unwind field set was missed. Example: UNWIND name, salary");
    }
  }

  protected void parseOrderBy() {
    parserRequiredKeyword(KEYWORD_BY);

    String fieldOrdering = null;

    orderedFields = new ArrayList<OPair<String, String>>();
    while (!parserIsEnded() && (orderedFields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");

      parserOptionalWord(true);

      final String word = parserGetLastWord();

      if (word.length() == 0)
      // END CLAUSE: SET AS ASC BY DEFAULT
      {
        fieldOrdering = KEYWORD_ASC;
      } else if (word.equals(KEYWORD_LIMIT) || word.equals(KEYWORD_SKIP) || word.equals(KEYWORD_OFFSET)) {
        // NEXT CLAUSE: SET AS ASC BY DEFAULT
        fieldOrdering = KEYWORD_ASC;
        parserGoBack();
      } else {
        if (word.equals(KEYWORD_ASC)) {
          fieldOrdering = KEYWORD_ASC;
        } else if (word.equals(KEYWORD_DESC)) {
          fieldOrdering = KEYWORD_DESC;
        } else {
          throwParsingException("Ordering mode '" + word + "' not supported. Valid is 'ASC', 'DESC' or nothing ('ASC' by default)");
        }
      }

      orderedFields.add(new OPair<String, String>(fieldName, fieldOrdering));
      parserSkipWhiteSpaces();
    }

    if (orderedFields.size() == 0) {
      throwParsingException("Order by field set was missed. Example: ORDER BY name ASC, salary DESC");
    }
  }

  @Override
  protected void searchInClasses() {
    final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();

    if (!searchForIndexes(cls) && !searchForSubclassIndexes(cls)) {
      // CHECK FOR INVERSE ORDER
      final boolean browsingOrderAsc = !(orderedFields.size() == 1 && orderedFields.get(0).getKey().equalsIgnoreCase("@rid")
          && orderedFields.get(0).getValue().equalsIgnoreCase("DESC"));
      super.searchInClasses(browsingOrderAsc);
    }
  }

  protected int parseProjections() {
    if (!parserOptionalKeyword(KEYWORD_SELECT))
      return -1;

    int upperBound = OStringSerializerHelper.getLowerIndexOfKeywords(parserTextUpperCase, parserGetCurrentPosition(), KEYWORD_FROM,
        KEYWORD_LET);
    if (upperBound == -1)
      // UP TO THE END
      upperBound = parserText.length();

    int lastRealPositionProjection = -1;

    int currPos = parserGetCurrentPosition();
    if (currPos == -1)
      return -1;

    final String projectionString = parserText.substring(currPos, upperBound);
    if (projectionString.trim().length() > 0) {
      // EXTRACT PROJECTIONS
      projections = new LinkedHashMap<String, Object>();
      projectionDefinition = new LinkedHashMap<String, String>();

      final List<String> items = OStringSerializerHelper.smartSplit(projectionString, ',');

      int endPos;
      for (String projectionItem : items) {
        String projection = OStringSerializerHelper.smartTrim(projectionItem.trim(), true, true);

        if (projectionDefinition == null)
          throw new OCommandSQLParsingException("Projection not allowed with FLATTEN() and EXPAND() operators");

        final List<String> words = OStringSerializerHelper.smartSplit(projection, ' ');

        String fieldName;
        if (words.size() > 1 && words.get(1).trim().equalsIgnoreCase(KEYWORD_AS)) {
          // FOUND AS, EXTRACT ALIAS
          if (words.size() < 3)
            throw new OCommandSQLParsingException("Found 'AS' without alias");

          fieldName = words.get(2).trim();

          if (projectionDefinition.containsKey(fieldName))
            throw new OCommandSQLParsingException(
                "Field '" + fieldName + "' is duplicated in current SELECT, choose a different name");

          projection = words.get(0).trim();

          if (words.size() > 3)
            lastRealPositionProjection = projectionString.indexOf(words.get(3));
          else
            lastRealPositionProjection += projectionItem.length() + 1;

        } else {
          // EXTRACT THE FIELD NAME WITHOUT FUNCTIONS AND/OR LINKS
          projection = words.get(0);
          fieldName = projection;

          lastRealPositionProjection = projectionString.indexOf(fieldName) + fieldName.length() + 1;

          if (fieldName.charAt(0) == '@')
            fieldName = fieldName.substring(1);

          endPos = extractProjectionNameSubstringEndPosition(fieldName);

          if (endPos > -1)
            fieldName = fieldName.substring(0, endPos);

          // FIND A UNIQUE NAME BY ADDING A COUNTER
          for (int fieldIndex = 2; projectionDefinition.containsKey(fieldName); ++fieldIndex)
            fieldName += fieldIndex;
        }

        final String p = upperCase(projection);
        if (p.startsWith("FLATTEN(") || p.startsWith("EXPAND(")) {
          if (p.startsWith("FLATTEN("))
            OLogManager.instance().debug(this, "FLATTEN() operator has been replaced by EXPAND()");

          List<String> pars = OStringSerializerHelper.getParameters(projection);
          if (pars.size() != 1)
            throw new OCommandSQLParsingException(
                "EXPAND/FLATTEN operators expects the field name as parameter. Example EXPAND( out )");

          expandTarget = OSQLHelper.parseValue(this, pars.get(0).trim(), context);

          // BY PASS THIS AS PROJECTION BUT TREAT IT AS SPECIAL
          projectionDefinition = null;
          projections = null;

          if (groupedResult == null && expandTarget instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) expandTarget).aggregateResults())
            groupedResult = new LinkedHashMap<Object, ORuntimeResult>();

          continue;
        }

        fieldName = OStringSerializerHelper.getStringContent(fieldName);

        projectionDefinition.put(fieldName, projection);
      }

      if (projectionDefinition != null
          && (projectionDefinition.size() > 1 || !projectionDefinition.values().iterator().next().equals("*"))) {
        projections = createProjectionFromDefinition();

        for (Object p : projections.values()) {

          if (groupedResult == null && p instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) p).aggregateResults()) {
            // AGGREGATE IT
            getProjectionGroup(null);
            break;
          }
        }

      } else {
        // TREATS SELECT * AS NO PROJECTION
        projectionDefinition = null;
        projections = null;
      }
    }

    if (upperBound < parserText.length() - 1)
      parserSetCurrentPosition(upperBound);
    else if (lastRealPositionProjection > -1)
      parserMoveCurrentPosition(lastRealPositionProjection);
    else
      parserSetEndOfText();

    return parserGetCurrentPosition();
  }

  protected Map<String, Object> createProjectionFromDefinition() {
    if (projectionDefinition == null) {
      return new LinkedHashMap<String, Object>();
    }

    final Map<String, Object> projections = new LinkedHashMap<String, Object>(projectionDefinition.size());
    for (Entry<String, String> p : projectionDefinition.entrySet()) {
      final Object projectionValue = OSQLHelper.parseValue(this, p.getValue(), context);
      projections.put(p.getKey(), projectionValue);
    }
    return projections;
  }

  protected int extractProjectionNameSubstringEndPosition(final String projection) {
    int endPos;
    final int pos1 = projection.indexOf('.');
    final int pos2 = projection.indexOf('(');
    final int pos3 = projection.indexOf('[');
    if (pos1 > -1 && pos2 == -1 && pos3 == -1) {
      endPos = pos1;
    } else if (pos2 > -1 && pos1 == -1 && pos3 == -1) {
      endPos = pos2;
    } else if (pos3 > -1 && pos1 == -1 && pos2 == -1) {
      endPos = pos3;
    } else if (pos1 > -1 && pos2 > -1 && pos3 == -1) {
      endPos = Math.min(pos1, pos2);
    } else if (pos2 > -1 && pos3 > -1 && pos1 == -1) {
      endPos = Math.min(pos2, pos3);
    } else if (pos1 > -1 && pos3 > -1 && pos2 == -1) {
      endPos = Math.min(pos1, pos3);
    } else if (pos1 > -1 && pos2 > -1 && pos3 > -1) {
      endPos = Math.min(pos1, pos2);
      endPos = Math.min(endPos, pos3);
    } else {
      endPos = -1;
    }
    return endPos;
  }

  /**
   * Parses the fetchplan keyword if found.
   */
  protected boolean parseFetchplan(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_FETCHPLAN)) {
      return false;
    }

    parserSkipWhiteSpaces();
    int start = parserGetCurrentPosition();

    parserNextWord(true);
    int end = parserGetCurrentPosition();
    parserSkipWhiteSpaces();

    int position = parserGetCurrentPosition();
    while (!parserIsEnded()) {
      final String word = OStringSerializerHelper.getStringContent(parserNextWord(true));
      if (!OPatternConst.PATTERN_FETCH_PLAN.matcher(word).matches()) {
        break;
      }

      end = parserGetCurrentPosition();
      parserSkipWhiteSpaces();
      position = parserGetCurrentPosition();
    }

    parserSetCurrentPosition(position);

    if (end < 0) {
      fetchPlan = OStringSerializerHelper.getStringContent(parserText.substring(start));
    } else {
      fetchPlan = OStringSerializerHelper.getStringContent(parserText.substring(start, end));
    }

    request.setFetchPlan(fetchPlan);

    return true;
  }

  protected boolean optimizeExecution() {
    if (compiledFilter != null) {
      mergeRangeConditionsToBetweenOperators(compiledFilter);
    }

    if ((compiledFilter == null || (compiledFilter.getRootCondition() == null)) && groupByFields == null && projections != null
        && projections.size() == 1) {

      final long startOptimization = System.currentTimeMillis();
      try {

        final Entry<String, Object> entry = projections.entrySet().iterator().next();

        if (entry.getValue() instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
          if (rf.function instanceof OSQLFunctionCount && rf.configuredParameters.length == 1
              && "*".equals(rf.configuredParameters[0])) {

            boolean restrictedClasses = false;
            final OSecurityUser user = getDatabase().getUser();

            if (parsedTarget.getTargetClasses() != null && user != null
                && user.checkIfAllowed(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ) == null) {
              for (OClass cls : parsedTarget.getTargetClasses().keySet()) {
                if (cls.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
                  restrictedClasses = true;
                  break;
                }
              }
            }

            if (!restrictedClasses) {
              long count = 0;

              if (parsedTarget.getTargetClasses() != null) {
                final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();
                count = cls.count();
              } else if (parsedTarget.getTargetClusters() != null) {
                for (String cluster : parsedTarget.getTargetClusters().keySet()) {
                  count += getDatabase().countClusterElements(cluster);
                }
              } else if (parsedTarget.getTargetIndex() != null) {
                count += getDatabase().getMetadata().getIndexManager().getIndex(parsedTarget.getTargetIndex()).getSize();
              } else {
                final Iterable<? extends OIdentifiable> recs = parsedTarget.getTargetRecords();
                if (recs != null) {
                  if (recs instanceof Collection<?>)
                    count += ((Collection<?>) recs).size();
                  else {
                    for (Object o : recs)
                      count++;
                  }
                }

              }

              if (tempResult == null)
                tempResult = new ArrayList<OIdentifiable>();
              ((Collection<OIdentifiable>) tempResult).add(new ODocument().field(entry.getKey(), count));
              return true;
            }
          }
        }

      } finally {
        context.setVariable("optimizationElapsed", (System.currentTimeMillis() - startOptimization));
      }
    }

    return false;
  }

  protected void revertSubclassesProfiler(final OCommandContext iContext, int num) {
    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      profiler.updateCounter(profiler.getDatabaseMetric(getDatabase().getName(), "query.indexUseAttemptedAndReverted"),
          "Reverted index usage in query", num);
    }
  }

  protected void revertProfiler(final OCommandContext iContext, final OIndex<?> index, final List<Object> keyParams,
      final OIndexDefinition indexDefinition) {
    if (iContext.isRecordingMetrics()) {
      iContext.updateMetric("compositeIndexUsed", -1);
    }

    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      profiler.updateCounter(profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"), "Used index in query", -1);

      int params = indexDefinition.getParamCount();
      if (params > 1) {
        final String profiler_prefix = profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");

        profiler.updateCounter(profiler_prefix, "Used composite index in query", -1);
        profiler.updateCounter(profiler_prefix + "." + params, "Used composite index in query with " + params + " params", -1);
        profiler.updateCounter(profiler_prefix + "." + params + '.' + keyParams.size(),
            "Used composite index in query with " + params + " params and " + keyParams.size() + " keys", -1);
      }
    }
  }

  /**
   * Parses the NOCACHE keyword if found.
   */
  protected boolean parseNoCache(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_NOCACHE))
      return false;

    noCache = true;
    return true;
  }

  private void mergeRangeConditionsToBetweenOperators(OSQLFilter filter) {
    OSQLFilterCondition condition = filter.getRootCondition();

    OSQLFilterCondition newCondition = convertToBetweenClause(condition);
    if (newCondition != null) {
      filter.setRootCondition(newCondition);
      metricRecorder.recordRangeQueryConvertedInBetween();
      return;
    }

    mergeRangeConditionsToBetweenOperators(condition);
  }

  private void mergeRangeConditionsToBetweenOperators(OSQLFilterCondition condition) {
    if (condition == null) {
      return;
    }

    OSQLFilterCondition newCondition;

    if (condition.getLeft() instanceof OSQLFilterCondition) {
      OSQLFilterCondition leftCondition = (OSQLFilterCondition) condition.getLeft();
      newCondition = convertToBetweenClause(leftCondition);

      if (newCondition != null) {
        condition.setLeft(newCondition);
        metricRecorder.recordRangeQueryConvertedInBetween();
      } else {
        mergeRangeConditionsToBetweenOperators(leftCondition);
      }
    }

    if (condition.getRight() instanceof OSQLFilterCondition) {
      OSQLFilterCondition rightCondition = (OSQLFilterCondition) condition.getRight();

      newCondition = convertToBetweenClause(rightCondition);
      if (newCondition != null) {
        condition.setRight(newCondition);
        metricRecorder.recordRangeQueryConvertedInBetween();
      } else {
        mergeRangeConditionsToBetweenOperators(rightCondition);
      }
    }
  }

  private OSQLFilterCondition convertToBetweenClause(OSQLFilterCondition condition) {
    if (condition == null) {
      return null;
    }

    final Object right = condition.getRight();
    final Object left = condition.getLeft();

    final OQueryOperator operator = condition.getOperator();
    if (!(operator instanceof OQueryOperatorAnd)) {
      return null;
    }

    if (!(right instanceof OSQLFilterCondition)) {
      return null;
    }

    if (!(left instanceof OSQLFilterCondition)) {
      return null;
    }

    String rightField;

    final OSQLFilterCondition rightCondition = (OSQLFilterCondition) right;
    final OSQLFilterCondition leftCondition = (OSQLFilterCondition) left;

    if (rightCondition.getLeft() instanceof OSQLFilterItemField && rightCondition.getRight() instanceof OSQLFilterItemField) {
      return null;
    }

    if (!(rightCondition.getLeft() instanceof OSQLFilterItemField) && !(rightCondition.getRight() instanceof OSQLFilterItemField)) {
      return null;
    }

    if (leftCondition.getLeft() instanceof OSQLFilterItemField && leftCondition.getRight() instanceof OSQLFilterItemField) {
      return null;
    }

    if (!(leftCondition.getLeft() instanceof OSQLFilterItemField) && !(leftCondition.getRight() instanceof OSQLFilterItemField)) {
      return null;
    }

    final List<Object> betweenBoundaries = new ArrayList<Object>();

    if (rightCondition.getLeft() instanceof OSQLFilterItemField) {
      OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getLeft();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      rightField = itemField.getRoot();
      betweenBoundaries.add(rightCondition.getRight());
    } else if (rightCondition.getRight() instanceof OSQLFilterItemField) {
      OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getRight();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      rightField = itemField.getRoot();
      betweenBoundaries.add(rightCondition.getLeft());
    } else {
      return null;
    }

    betweenBoundaries.add("and");

    String leftField;
    if (leftCondition.getLeft() instanceof OSQLFilterItemField) {
      OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getLeft();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      leftField = itemField.getRoot();
      betweenBoundaries.add(leftCondition.getRight());
    } else if (leftCondition.getRight() instanceof OSQLFilterItemField) {
      OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getRight();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      leftField = itemField.getRoot();
      betweenBoundaries.add(leftCondition.getLeft());
    } else {
      return null;
    }

    if (!leftField.equalsIgnoreCase(rightField)) {
      return null;
    }

    final OQueryOperator rightOperator = ((OSQLFilterCondition) right).getOperator();
    final OQueryOperator leftOperator = ((OSQLFilterCondition) left).getOperator();

    if ((rightOperator instanceof OQueryOperatorMajor || rightOperator instanceof OQueryOperatorMajorEquals)
        && (leftOperator instanceof OQueryOperatorMinor || leftOperator instanceof OQueryOperatorMinorEquals)) {

      final OQueryOperatorBetween between = new OQueryOperatorBetween();

      if (rightOperator instanceof OQueryOperatorMajor) {
        between.setLeftInclusive(false);
      }

      if (leftOperator instanceof OQueryOperatorMinor) {
        between.setRightInclusive(false);
      }

      return new OSQLFilterCondition(new OSQLFilterItemField(this, leftField), between, betweenBoundaries.toArray());
    }

    if ((leftOperator instanceof OQueryOperatorMajor || leftOperator instanceof OQueryOperatorMajorEquals)
        && (rightOperator instanceof OQueryOperatorMinor || rightOperator instanceof OQueryOperatorMinorEquals)) {
      final OQueryOperatorBetween between = new OQueryOperatorBetween();

      if (leftOperator instanceof OQueryOperatorMajor) {
        between.setLeftInclusive(false);
      }

      if (rightOperator instanceof OQueryOperatorMinor) {
        between.setRightInclusive(false);
      }

      Collections.reverse(betweenBoundaries);

      return new OSQLFilterCondition(new OSQLFilterItemField(this, leftField), between, betweenBoundaries.toArray());

    }

    return null;
  }

  public void initContext() {
    if (context == null) {
      context = new OBasicCommandContext();
    }

    metricRecorder.setContext(context);
  }

  private boolean fetchFromTarget(final Iterator<? extends OIdentifiable> iTarget) {
    fetchLimit = getQueryFetchLimit();

    final long startFetching = System.currentTimeMillis();

    try {
      if (parallel) {
        parallelExec(iTarget);
      } else {
        int queryScanThresholdWarning = OGlobalConfiguration.QUERY_SCAN_THRESHOLD_TIP.getValueAsInteger();

        // BROWSE, UNMARSHALL AND FILTER ALL THE RECORDS ON CURRENT THREAD
        for (int browsed = 0; iTarget.hasNext(); browsed++) {
          final OIdentifiable next = iTarget.next();
          if (next == null)
            break;

          final ORID identity = next.getIdentity();

          if (uniqueResult != null) {
            if (uniqueResult.contains(identity))
              continue;

            if (identity.isValid())
              uniqueResult.add(identity);
          }

          if (!executeSearchRecord(next))
            return false;

          if (queryScanThresholdWarning > 0 && browsed > queryScanThresholdWarning && compiledFilter != null) {
            reportTip(String.format(
                "Query '%s' fetched more than %d records: to speed up the execution, create an index or change the query to use an existent index",
                parserText, queryScanThresholdWarning));
            queryScanThresholdWarning = 0;
          }
        }
      }
      return true;

    } finally {
      context.setVariable("fetchingFromTargetElapsed", (System.currentTimeMillis() - startFetching));
    }
  }

  private boolean parseParallel(String w) {
    return w.equals(KEYWORD_PARALLEL);
  }

  private void parallelExec(final Iterator<? extends OIdentifiable> iTarget) {
    final OResultSet result = (OResultSet) getResult();

    // BROWSE ALL THE RECORDS ON CURRENT THREAD BUT DELEGATE UNMARSHALLING AND FILTER TO A THREAD POOL
    final ODatabaseDocumentInternal db = getDatabase();

    if (limit > -1) {
      if (result != null) {
        result.setLimit(limit);
      }
    }

    final int cores = Runtime.getRuntime().availableProcessors();
    OLogManager.instance().debug(this, "Parallel query against %d threads", cores);

    executing = true;
    final List<Future<?>> jobs = new ArrayList<Future<?>>();

    // BROWSE ALL THE RECORDS AND PUT THE RECORD INTO THE QUEUE
    while (executing && iTarget.hasNext()) {
      final OIdentifiable next = iTarget.next();

      if (next == null) {
        break;
      }

      final Runnable job = new Runnable() {
        @Override
        public void run() {
          ODatabaseRecordThreadLocal.INSTANCE.set(db);

          if (!executeSearchRecord(next)) {
            executing = false;
          }
        }
      };

      jobs.add(Orient.instance().submit(job));
    }

    if (OLogManager.instance().isDebugEnabled()) {
      OLogManager.instance().debug(this, "Parallel query '%s' split in %d jobs, waiting for completion...", parserText,
          jobs.size());
    }

    int processed = 0;
    int total = jobs.size();
    try {
      for (Future<?> j : jobs) {
        j.get();
        processed++;

        if (OLogManager.instance().isDebugEnabled()) {
          if (processed % (total / 10) == 0) {
            OLogManager.instance().debug(this, "Executed parallel query %d/%d", processed, total);
          }
        }
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on executing parallel query: %s", e, parserText);
    }

    if (OLogManager.instance().isDebugEnabled()) {
      OLogManager.instance().debug(this, "Parallel query '%s' completed", parserText);
    }
  }

  private int getQueryFetchLimit() {
    final int sqlLimit;
    final int requestLimit;

    if (limit > -1) {
      sqlLimit = limit;
    } else {
      sqlLimit = -1;
    }

    if (request.getLimit() > -1) {
      requestLimit = request.getLimit();
    } else {
      requestLimit = -1;
    }

    if (sqlLimit == -1) {
      return requestLimit;
    }

    if (requestLimit == -1) {
      return sqlLimit;
    }

    return Math.min(sqlLimit, requestLimit);
  }

  private OIndexCursor tryGetOptimizedSortCursor(final OClass iSchemaClass) {
    if (orderedFields.size() == 0) {
      return null;
    } else {
      return getOptimizedSortCursor(iSchemaClass);
    }
  }

  private boolean tryOptimizeSort(final OClass iSchemaClass) {
    if (orderedFields.size() == 0) {
      return false;
    } else {
      return optimizeSort(iSchemaClass);
    }
  }

  private boolean searchForSubclassIndexes(final OClass iSchemaClass) {
    Collection<OClass> subclasses = iSchemaClass.getSubclasses();
    if (subclasses.size() == 0) {
      return false;
    }

    OOrderBy order = new OOrderBy();
    order.setItems(new ArrayList<OOrderByItem>());
    if (this.orderedFields != null) {
      for (OPair<String, String> pair : this.orderedFields) {
        OOrderByItem item = new OOrderByItem();
        item.setRecordAttr(pair.getKey());
        if (pair.getValue() == null) {
          item.setType(OOrderByItem.ASC);
        } else {
          item.setType(pair.getValue().toUpperCase().equals("DESC") ? OOrderByItem.DESC : OOrderByItem.ASC);
        }
        order.getItems().add(item);
      }
    }
    OSortedMultiIterator<OIdentifiable> cursor = new OSortedMultiIterator<OIdentifiable>(order);
    boolean fullySorted = true;

    if (!iSchemaClass.isAbstract()) {
      Iterator<OIdentifiable> parentClassIterator = (Iterator<OIdentifiable>) searchInClasses(iSchemaClass, false, true);
      if (parentClassIterator.hasNext()) {
        cursor.add(parentClassIterator);
        fullySorted = false;
      }
    }

    if (uniqueResult != null) {
      uniqueResult.clear();
    }

    int attempted = 0;
    for (OClass subclass : subclasses) {
      List<OIndexCursor> subcursors = getIndexCursors(subclass);
      fullySorted = fullySorted && fullySortedByIndex;
      if (subcursors == null || subcursors.size() == 0) {
        if (attempted > 0) {
          revertSubclassesProfiler(context, attempted);
        }
        return false;
      }
      for (OIndexCursor c : subcursors) {
        if (!fullySortedByIndex) {
          // TODO sort every iterator
        }
        attempted++;
        cursor.add(c);
      }

    }
    fullySortedByIndex = fullySorted;

    uniqueResult = new HashSet<ORID>();

    fetchFromTarget(cursor);

    if (uniqueResult != null) {
      uniqueResult.clear();
    }
    uniqueResult = null;

    return true;
  }

  @SuppressWarnings("rawtypes")
  private List<OIndexCursor> getIndexCursors(final OClass iSchemaClass) {

    final ODatabaseDocument database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iSchemaClass.getName().toLowerCase());

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null) {
      OIndexCursor cursor = tryGetOptimizedSortCursor(iSchemaClass);
      if (cursor == null) {
        return null;
      }
      List<OIndexCursor> result = new ArrayList<OIndexCursor>();
      result.add(cursor);
      return result;

    }

    // the main condition is a set of sub-conditions separated by OR operators
    final List<List<OIndexSearchResult>> conditionHierarchy = filterAnalyzer.analyzeMainCondition(compiledFilter.getRootCondition(),
        iSchemaClass, context);
    if (conditionHierarchy == null)
      return null;

    List<OIndexCursor> cursors = new ArrayList<OIndexCursor>();

    boolean indexIsUsedInOrderBy = false;
    List<IndexUsageLog> indexUseAttempts = new ArrayList<IndexUsageLog>();
    // try {

    OIndexSearchResult lastSearchResult = null;
    for (List<OIndexSearchResult> indexSearchResults : conditionHierarchy) {
      // go through all variants to choose which one can be used for index search.
      boolean indexUsed = false;
      for (final OIndexSearchResult searchResult : indexSearchResults) {
        lastSearchResult = searchResult;
        final List<OIndex<?>> involvedIndexes = filterAnalyzer.getInvolvedIndexes(iSchemaClass, searchResult);

        Collections.sort(involvedIndexes, new IndexComparator());

        // go through all possible index for given set of fields.
        for (final OIndex index : involvedIndexes) {
          if (index.isRebuiding()) {
            continue;
          }

          final OIndexDefinition indexDefinition = index.getDefinition();

          if (searchResult.containsNullValues && indexDefinition.isNullValuesIgnored()) {
            continue;
          }

          final OQueryOperator operator = searchResult.lastOperator;

          // we need to test that last field in query subset and field in index that has the same position
          // are equals.
          if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
            final String lastFiled = searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
            final String relatedIndexField = indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
            if (!lastFiled.equals(relatedIndexField)) {
              continue;
            }
          }

          final int searchResultFieldsCount = searchResult.fields().size();
          final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
          // We get only subset contained in processed sub query.
          for (final String fieldName : indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
            final Object fieldValue = searchResult.fieldValuePairs.get(fieldName);
            if (fieldValue instanceof OSQLQuery<?>) {
              return null;
            }

            if (fieldValue != null) {
              keyParams.add(fieldValue);
            } else {
              if (searchResult.lastValue instanceof OSQLQuery<?>) {
                return null;
              }

              keyParams.add(searchResult.lastValue);
            }
          }

          metricRecorder.recordInvolvedIndexesMetric(index);

          OIndexCursor cursor;
          indexIsUsedInOrderBy = orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)
              && !(index.getInternal() instanceof OChainedIndexProxy);
          try {
            boolean ascSortOrder = !indexIsUsedInOrderBy || orderedFields.get(0).getValue().equals(KEYWORD_ASC);

            if (indexIsUsedInOrderBy) {
              fullySortedByIndex = indexDefinition.getFields().size() >= orderedFields.size() && conditionHierarchy.size() == 1;
            }

            context.setVariable("$limit", limit);

            cursor = operator.executeIndexQuery(context, index, keyParams, ascSortOrder);

          } catch (OIndexEngineException e) {
            throw e;
          } catch (Exception e) {
            OLogManager.instance().error(this,
                "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                e, index.getName(), request != null && request.getText() != null ? request.getText() : "");

            fullySortedByIndex = false;
            cursors.clear();
            return null;
          }

          if (cursor == null) {
            continue;
          }
          cursors.add(cursor);
          indexUseAttempts.add(new IndexUsageLog(index, keyParams, indexDefinition));
          indexUsed = true;
          break;
        }
        if (indexUsed) {
          break;
        }
      }
      if (!indexUsed) {
        OIndexCursor cursor = tryGetOptimizedSortCursor(iSchemaClass);
        if (cursor == null) {
          return null;
        }
        List<OIndexCursor> result = new ArrayList<OIndexCursor>();
        result.add(cursor);
        return result;
      }
    }

    if (cursors.size() == 0 || lastSearchResult == null) {
      return null;
    }
    // if (cursors.size() == 1 && canOptimize(conditionHierarchy)) {
    // filterOptimizer.optimize(compiledFilter, lastSearchResult);
    // }

    metricRecorder.recordOrderByOptimizationMetric(indexIsUsedInOrderBy, this.fullySortedByIndex);

    indexUseAttempts.clear();

    return cursors;

    // } finally {
    // for (IndexUsageLog wastedIndexUsage : indexUseAttempts) {
    // revertProfiler(context, wastedIndexUsage.index, wastedIndexUsage.keyParams, wastedIndexUsage.indexDefinition);
    // }//TODO profiler
    // }
  }

  @SuppressWarnings("rawtypes")
  private boolean searchForIndexes(final OClass iSchemaClass) {
    if (uniqueResult != null)
      uniqueResult.clear();

    final ODatabaseDocument database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iSchemaClass.getName().toLowerCase());

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null) {
      return tryOptimizeSort(iSchemaClass);
    }

    // the main condition is a set of sub-conditions separated by OR operators
    final List<List<OIndexSearchResult>> conditionHierarchy = filterAnalyzer.analyzeMainCondition(compiledFilter.getRootCondition(),
        iSchemaClass, context);
    if (conditionHierarchy == null)
      return false;

    List<OIndexCursor> cursors = new ArrayList<OIndexCursor>();

    boolean indexIsUsedInOrderBy = false;
    List<IndexUsageLog> indexUseAttempts = new ArrayList<IndexUsageLog>();
    try {

      OIndexSearchResult lastSearchResult = null;
      for (List<OIndexSearchResult> indexSearchResults : conditionHierarchy) {
        // go through all variants to choose which one can be used for index search.
        boolean indexUsed = false;
        for (final OIndexSearchResult searchResult : indexSearchResults) {
          lastSearchResult = searchResult;
          final List<OIndex<?>> involvedIndexes = filterAnalyzer.getInvolvedIndexes(iSchemaClass, searchResult);

          Collections.sort(involvedIndexes, new IndexComparator());

          // go through all possible index for given set of fields.
          for (final OIndex index : involvedIndexes) {
            if (index.isRebuiding()) {
              continue;
            }

            final OIndexDefinition indexDefinition = index.getDefinition();

            if (searchResult.containsNullValues && indexDefinition.isNullValuesIgnored()) {
              continue;
            }

            final OQueryOperator operator = searchResult.lastOperator;

            // we need to test that last field in query subset and field in index that has the same position
            // are equals.
            if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
              final String lastFiled = searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
              final String relatedIndexField = indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
              if (!lastFiled.equals(relatedIndexField)) {
                continue;
              }
            }

            final int searchResultFieldsCount = searchResult.fields().size();
            final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
            // We get only subset contained in processed sub query.
            for (final String fieldName : indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
              final Object fieldValue = searchResult.fieldValuePairs.get(fieldName);
              if (fieldValue instanceof OSQLQuery<?>) {
                return false;
              }

              if (fieldValue != null) {
                keyParams.add(fieldValue);
              } else {
                if (searchResult.lastValue instanceof OSQLQuery<?>) {
                  return false;
                }

                keyParams.add(searchResult.lastValue);
              }
            }

            metricRecorder.recordInvolvedIndexesMetric(index);

            OIndexCursor cursor;
            indexIsUsedInOrderBy = orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)
                && !(index.getInternal() instanceof OChainedIndexProxy);
            try {
              boolean ascSortOrder = !indexIsUsedInOrderBy || orderedFields.get(0).getValue().equals(KEYWORD_ASC);

              if (indexIsUsedInOrderBy) {
                fullySortedByIndex = indexDefinition.getFields().size() >= orderedFields.size() && conditionHierarchy.size() == 1;
              }

              context.setVariable("$limit", limit);

              cursor = operator.executeIndexQuery(context, index, keyParams, ascSortOrder);

            } catch (OIndexEngineException e) {
              throw e;
            } catch (Exception e) {
              OLogManager.instance().error(this,
                  "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                  e, index.getName(), request != null && request.getText() != null ? request.getText() : "");

              fullySortedByIndex = false;
              cursors.clear();
              return false;
            }

            if (cursor == null) {
              continue;
            }
            cursors.add(cursor);
            indexUseAttempts.add(new IndexUsageLog(index, keyParams, indexDefinition));
            indexUsed = true;
            break;
          }
          if (indexUsed) {
            break;
          }
        }
        if (!indexUsed) {
          return tryOptimizeSort(iSchemaClass);
        }
      }

      if (cursors.size() == 0 || lastSearchResult == null) {
        return false;
      }
      if (cursors.size() == 1 && canOptimize(conditionHierarchy)) {
        filterOptimizer.optimize(compiledFilter, lastSearchResult);
      }

      uniqueResult = new HashSet<ORID>();
      for (OIndexCursor cursor : cursors) {
        if (!fetchValuesFromIndexCursor(cursor)) {
          break;
        }
      }
      uniqueResult.clear();
      uniqueResult = null;

      metricRecorder.recordOrderByOptimizationMetric(indexIsUsedInOrderBy, this.fullySortedByIndex);

      indexUseAttempts.clear();
      return true;
    } finally {
      for (IndexUsageLog wastedIndexUsage : indexUseAttempts) {
        revertProfiler(context, wastedIndexUsage.index, wastedIndexUsage.keyParams, wastedIndexUsage.indexDefinition);
      }
    }
  }

  private boolean canOptimize(List<List<OIndexSearchResult>> conditionHierarchy) {
    if (conditionHierarchy.size() > 1) {
      return false;
    }
    for (List<OIndexSearchResult> subCoditions : conditionHierarchy) {
      if (subCoditions.size() > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Use index to order documents by provided fields.
   *
   * @param iSchemaClass
   *          where search for indexes for optimization.
   * @return true if execution was optimized
   */
  private boolean optimizeSort(OClass iSchemaClass) {
    OIndexCursor cursor = getOptimizedSortCursor(iSchemaClass);
    if (cursor != null) {
      fetchValuesFromIndexCursor(cursor);
      return true;
    }
    return false;
  }

  private OIndexCursor getOptimizedSortCursor(OClass iSchemaClass) {
    final List<String> fieldNames = new ArrayList<String>();

    for (OPair<String, String> pair : orderedFields) {
      fieldNames.add(pair.getKey());
    }

    final Set<OIndex<?>> indexes = iSchemaClass.getInvolvedIndexes(fieldNames);

    for (OIndex<?> index : indexes) {
      if (orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)) {
        final boolean ascSortOrder = orderedFields.get(0).getValue().equals(KEYWORD_ASC);

        final Object key;
        if (ascSortOrder) {
          key = index.getFirstKey();
        } else {
          key = index.getLastKey();
        }

        if (key == null) {
          return null;
        }

        fullySortedByIndex = true;

        if (context.isRecordingMetrics()) {
          context.setVariable("indexIsUsedInOrderBy", true);
          context.setVariable("fullySortedByIndex", fullySortedByIndex);

          Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
          if (idxNames == null) {
            idxNames = new HashSet<String>();
            context.setVariable("involvedIndexes", idxNames);
          }

          idxNames.add(index.getName());
        }

        final OIndexCursor cursor;
        if (ascSortOrder) {
          cursor = index.iterateEntriesMajor(key, true, true);
        } else {
          cursor = index.iterateEntriesMinor(key, true, false);
        }

        return cursor;
      }
    }

    metricRecorder.recordOrderByOptimizationMetric(false, this.fullySortedByIndex);
    return null;
  }

  private boolean fetchValuesFromIndexCursor(final OIndexCursor cursor) {
    int needsToFetch;
    if (fetchLimit > 0) {
      needsToFetch = fetchLimit + skip;
    } else {
      needsToFetch = -1;
    }

    cursor.setPrefetchSize(needsToFetch);
    return fetchFromTarget(cursor);
  }

  private void fetchEntriesFromIndexCursor(final OIndexCursor cursor) {
    int needsToFetch;
    if (fetchLimit > 0) {
      needsToFetch = fetchLimit + skip;
    } else {
      needsToFetch = -1;
    }

    cursor.setPrefetchSize(needsToFetch);

    Entry<Object, OIdentifiable> entryRecord = cursor.nextEntry();
    if (needsToFetch > 0) {
      needsToFetch--;
    }

    while (entryRecord != null) {
      final ODocument doc = new ODocument().setOrdered(true);
      doc.field("key", entryRecord.getKey());
      doc.field("rid", entryRecord.getValue().getIdentity());
      ORecordInternal.unsetDirty(doc);

      if (!handleResult(doc))
      // LIMIT REACHED
      {
        break;
      }

      if (needsToFetch > 0) {
        needsToFetch--;
        cursor.setPrefetchSize(needsToFetch);
      }

      entryRecord = cursor.nextEntry();
    }
  }

  private boolean isRidOnlySort() {
    if (parsedTarget.getTargetClasses() != null && this.orderedFields.size() == 1
        && this.orderedFields.get(0).getKey().toLowerCase().equals("@rid")) {
      if (this.target != null && target instanceof ORecordIteratorClass) {
        return true;
      }
    }
    return false;
  }

  private void applyOrderBy() {
    if (orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort()) {
      return;
    }

    final long startOrderBy = System.currentTimeMillis();
    try {
      if (tempResult instanceof OMultiCollectionIterator) {
        final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
        for (OIdentifiable o : tempResult) {
          list.add(o);
        }
        tempResult = list;
      }
      tempResult = applySort((List<OIdentifiable>) tempResult, orderedFields, context);
      orderedFields.clear();
    } finally {
      metricRecorder.orderByElapsed(startOrderBy);
    }
  }

  private Iterable<OIdentifiable> applySort(List<OIdentifiable> iCollection, List<OPair<String, String>> iOrderFields,
      OCommandContext iContext) {

    ODocumentHelper.sort(iCollection, iOrderFields, iContext);
    return iCollection;
  }

  /**
   * Extract the content of collections and/or links and put it as result
   */
  private void applyExpand() {
    if (expandTarget == null) {
      return;
    }

    final long startExpand = System.currentTimeMillis();
    try {

      if (tempResult == null) {
        tempResult = new ArrayList<OIdentifiable>();
        if (expandTarget instanceof OSQLFilterItemVariable) {
          Object r = ((OSQLFilterItemVariable) expandTarget).getValue(null, null, context);
          if (r != null) {
            if (r instanceof OIdentifiable) {
              ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) r);
            } else if (OMultiValue.isMultiValue(r)) {
              for (Object o : OMultiValue.getMultiValueIterable(r)) {
                ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) o);
              }
            }
          }
        }
      } else {
        final OMultiCollectionIterator<OIdentifiable> finalResult = new OMultiCollectionIterator<OIdentifiable>();
        finalResult.setLimit(limit);
        for (OIdentifiable id : tempResult) {
          final Object fieldValue;
          if (expandTarget instanceof OSQLFilterItem) {
            fieldValue = ((OSQLFilterItem) expandTarget).getValue(id.getRecord(), null, context);
          } else if (expandTarget instanceof OSQLFunctionRuntime) {
            fieldValue = ((OSQLFunctionRuntime) expandTarget).getResult();
          } else {
            fieldValue = expandTarget.toString();
          }

          if (fieldValue != null) {
            if (fieldValue instanceof ODocument) {
              ArrayList<ODocument> partial = new ArrayList<ODocument>();
              partial.add((ODocument) fieldValue);
              finalResult.add(partial);
            } else if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray() || fieldValue instanceof Iterator<?>
                || fieldValue instanceof OIdentifiable || fieldValue instanceof ORidBag) {
              finalResult.add(fieldValue);
            } else if (fieldValue instanceof Map<?, ?>) {
              finalResult.add(((Map<?, OIdentifiable>) fieldValue).values());
            }
          }
        }
        tempResult = finalResult;
      }
    } finally {
      context.setVariable("expandElapsed", (System.currentTimeMillis() - startExpand));
    }

  }

  private void searchInIndex() {
    final OIndex<Object> index = (OIndex<Object>) getDatabase().getMetadata().getIndexManager()
        .getIndex(parsedTarget.getTargetIndex());

    if (index == null) {
      throw new OCommandExecutionException("Target index '" + parsedTarget.getTargetIndex() + "' not found");
    }

    boolean ascOrder = true;
    if (!orderedFields.isEmpty()) {
      if (orderedFields.size() != 1) {
        throw new OCommandExecutionException("Index can be ordered only by key field");
      }

      final String fieldName = orderedFields.get(0).getKey();
      if (!fieldName.equalsIgnoreCase("key")) {
        throw new OCommandExecutionException("Index can be ordered only by key field");
      }

      final String order = orderedFields.get(0).getValue();
      ascOrder = order.equalsIgnoreCase(KEYWORD_ASC);
    }

    // nothing was added yet, so index definition for manual index was not calculated
    if (index.getDefinition() == null) {
      return;
    }

    if (compiledFilter != null && compiledFilter.getRootCondition() != null) {
      if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString())) {
        throw new OCommandExecutionException("'Key' field is required for queries against indexes");
      }

      final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();

      if (indexOperator instanceof OQueryOperatorBetween) {
        final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();

        final OIndexCursor cursor = index.iterateEntriesBetween(getIndexKey(index.getDefinition(), values[0], context), true,
            getIndexKey(index.getDefinition(), values[2], context), true, ascOrder);
        fetchEntriesFromIndexCursor(cursor);
      } else if (indexOperator instanceof OQueryOperatorMajor) {
        final Object value = compiledFilter.getRootCondition().getRight();

        final OIndexCursor cursor = index.iterateEntriesMajor(getIndexKey(index.getDefinition(), value, context), false, ascOrder);
        fetchEntriesFromIndexCursor(cursor);
      } else if (indexOperator instanceof OQueryOperatorMajorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final OIndexCursor cursor = index.iterateEntriesMajor(getIndexKey(index.getDefinition(), value, context), true, ascOrder);
        fetchEntriesFromIndexCursor(cursor);

      } else if (indexOperator instanceof OQueryOperatorMinor) {
        final Object value = compiledFilter.getRootCondition().getRight();

        OIndexCursor cursor = index.iterateEntriesMinor(getIndexKey(index.getDefinition(), value, context), false, ascOrder);
        fetchEntriesFromIndexCursor(cursor);
      } else if (indexOperator instanceof OQueryOperatorMinorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();

        OIndexCursor cursor = index.iterateEntriesMinor(getIndexKey(index.getDefinition(), value, context), true, ascOrder);
        fetchEntriesFromIndexCursor(cursor);
      } else if (indexOperator instanceof OQueryOperatorIn) {
        final List<Object> origValues = (List<Object>) compiledFilter.getRootCondition().getRight();
        final List<Object> values = new ArrayList<Object>(origValues.size());
        for (Object val : origValues) {
          if (index.getDefinition() instanceof OCompositeIndexDefinition) {
            throw new OCommandExecutionException("Operator IN not supported yet.");
          }

          val = getIndexKey(index.getDefinition(), val, context);
          values.add(val);
        }

        OIndexCursor cursor = index.iterateEntries(values, true);
        fetchEntriesFromIndexCursor(cursor);
      } else {
        final Object right = compiledFilter.getRootCondition().getRight();
        Object keyValue = getIndexKey(index.getDefinition(), right, context);
        if (keyValue == null) {
          return;
        }

        final Object res;
        if (index.getDefinition().getParamCount() == 1) {
          // CONVERT BEFORE SEARCH IF NEEDED
          final OType type = index.getDefinition().getTypes()[0];
          keyValue = OType.convert(keyValue, type.getDefaultJavaType());

          res = index.get(keyValue);
        } else {
          final Object secondKey = getIndexKey(index.getDefinition(), right, context);
          if (keyValue instanceof OCompositeKey && secondKey instanceof OCompositeKey
              && ((OCompositeKey) keyValue).getKeys().size() == index.getDefinition().getParamCount()
              && ((OCompositeKey) secondKey).getKeys().size() == index.getDefinition().getParamCount()) {
            res = index.get(keyValue);
          } else {
            OIndexCursor cursor = index.iterateEntriesBetween(keyValue, true, secondKey, true, true);
            fetchEntriesFromIndexCursor(cursor);
            return;
          }

        }

        if (res != null) {
          if (res instanceof Collection<?>) {
            // MULTI VALUES INDEX
            for (final OIdentifiable r : (Collection<OIdentifiable>) res) {
              if (!handleResult(createIndexEntryAsDocument(keyValue, r.getIdentity())))
              // LIMIT REACHED
              {
                break;
              }
            }
          } else {
            // SINGLE VALUE INDEX
            handleResult(createIndexEntryAsDocument(keyValue, ((OIdentifiable) res).getIdentity()));
          }
        }
      }

    } else {
      if (isIndexSizeQuery()) {
        getProjectionGroup(null).applyValue(projections.keySet().iterator().next(), index.getSize());
        return;
      }

      if (isIndexKeySizeQuery()) {
        getProjectionGroup(null).applyValue(projections.keySet().iterator().next(), index.getKeySize());
        return;
      }

      final OIndexInternal<?> indexInternal = index.getInternal();
      if (indexInternal instanceof OSharedResource) {
        ((OSharedResource) indexInternal).acquireExclusiveLock();
      }

      try {
        // ADD ALL THE ITEMS AS RESULT
        if (ascOrder) {
          final Object firstKey = index.getFirstKey();
          if (firstKey == null) {
            return;
          }

          final OIndexCursor cursor = index.iterateEntriesMajor(firstKey, true, true);
          fetchEntriesFromIndexCursor(cursor);
        } else {
          final Object lastKey = index.getLastKey();
          if (lastKey == null) {
            return;
          }

          final OIndexCursor cursor = index.iterateEntriesMinor(lastKey, true, false);
          fetchEntriesFromIndexCursor(cursor);
        }
      } finally {
        if (indexInternal instanceof OSharedResource) {
          ((OSharedResource) indexInternal).releaseExclusiveLock();
        }
      }
    }
  }

  private boolean isIndexSizeQuery() {
    if (!(groupedResult != null && projections.entrySet().size() == 1)) {
      return false;
    }

    final Object projection = projections.values().iterator().next();
    if (!(projection instanceof OSQLFunctionRuntime)) {
      return false;
    }

    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
    return f.getRoot().equals(OSQLFunctionCount.NAME) && ((f.configuredParameters == null || f.configuredParameters.length == 0)
        || (f.configuredParameters.length == 1 && f.configuredParameters[0].equals("*")));
  }

  private boolean isIndexKeySizeQuery() {
    if (!(groupedResult != null && projections.entrySet().size() == 1)) {
      return false;
    }

    final Object projection = projections.values().iterator().next();
    if (!(projection instanceof OSQLFunctionRuntime)) {
      return false;
    }

    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
    if (!f.getRoot().equals(OSQLFunctionCount.NAME)) {
      return false;
    }

    if (!(f.configuredParameters != null && f.configuredParameters.length == 1
        && f.configuredParameters[0] instanceof OSQLFunctionRuntime)) {
      return false;
    }

    final OSQLFunctionRuntime fConfigured = (OSQLFunctionRuntime) f.configuredParameters[0];
    if (!fConfigured.getRoot().equals(OSQLFunctionDistinct.NAME)) {
      return false;
    }

    if (!(fConfigured.configuredParameters != null && fConfigured.configuredParameters.length == 1
        && fConfigured.configuredParameters[0] instanceof OSQLFilterItemField)) {
      return false;
    }

    final OSQLFilterItemField field = (OSQLFilterItemField) fConfigured.configuredParameters[0];
    return field.getRoot().equals("key");
  }

  private void handleNoTarget() {
    if (parsedTarget == null && expandTarget == null)
      // ONLY LET, APPLY TO THEM
      addResult(ORuntimeResult.createProjectionDocument(getTemporaryRIDCounter()));
  }

  private void handleGroupBy() {
    if (groupedResult != null && tempResult == null) {

      final long startGroupBy = System.currentTimeMillis();
      try {

        tempResult = new ArrayList<OIdentifiable>();

        for (Entry<Object, ORuntimeResult> g : groupedResult.entrySet()) {
          if (g.getKey() != null || (groupedResult.size() == 1 && groupByFields == null)) {
            final ODocument doc = g.getValue().getResult();
            if (doc != null && !doc.isEmpty()) {
              ((List<OIdentifiable>) tempResult).add(doc);
            }
          }
        }

      } finally {
        context.setVariable("groupByElapsed", (System.currentTimeMillis() - startGroupBy));
      }
    }
  }

  public void setProjections(final Map<String, Object> projections) {
    this.projections = projections;
  }

  public Map<String, String> getProjectionDefinition() {
    return projectionDefinition;
  }

  public void setProjectionDefinition(final Map<String, String> projectionDefinition) {
    this.projectionDefinition = projectionDefinition;
  }

  public void setOrderedFields(final List<OPair<String, String>> orderedFields) {
    this.orderedFields = orderedFields;
  }

  public void setGroupByFields(final List<String> groupByFields) {
    this.groupByFields = groupByFields;
  }

  public void setFetchLimit(final int fetchLimit) {
    this.fetchLimit = fetchLimit;
  }

  public void setFetchPlan(final String fetchPlan) {
    this.fetchPlan = fetchPlan;
  }

  public void setParallel(final boolean parallel) {
    this.parallel = parallel;
  }

  public void setNoCache(final boolean noCache) {
    this.noCache = noCache;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.READ;
  }

}
