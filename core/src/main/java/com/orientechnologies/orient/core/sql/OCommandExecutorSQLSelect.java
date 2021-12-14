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

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.collection.OSortedMultiIterator;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.stream.BreakingForEach;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.iterator.OIdentifiableIterator;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClusters;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
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
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorAnd;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import com.orientechnologies.orient.core.sql.parser.OOrderByItem;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and builds the meta
 * information needed by the execute(). If the query contains the ORDER BY clause, the results are
 * temporary collected internally, then ordered and finally returned all together to the listener.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSelect extends OCommandExecutorSQLResultsetAbstract
    implements OTemporaryRidGenerator {
  public static final String KEYWORD_SELECT = "SELECT";
  public static final String KEYWORD_ASC = "ASC";
  public static final String KEYWORD_DESC = "DESC";
  public static final String KEYWORD_ORDER = "ORDER";
  public static final String KEYWORD_BY = "BY";
  public static final String KEYWORD_GROUP = "GROUP";
  public static final String KEYWORD_UNWIND = "UNWIND";
  public static final String KEYWORD_FETCHPLAN = "FETCHPLAN";
  public static final String KEYWORD_NOCACHE = "NOCACHE";
  public static final String KEYWORD_FOREACH = "FOREACH";
  private static final String KEYWORD_AS = "AS";
  private static final String KEYWORD_PARALLEL = "PARALLEL";
  private static final int PARTIAL_SORT_BUFFER_THRESHOLD = 10000;
  private static final String NULL_VALUE = "null";

  private static class AsyncResult {
    private final OIdentifiable record;
    private final OCommandContext context;

    public AsyncResult(final ORecord iRecord, final OCommandContext iContext) {
      record = iRecord;
      context = iContext;
    }
  }

  private static final AsyncResult PARALLEL_END_EXECUTION_THREAD = new AsyncResult(null, null);

  private final OOrderByOptimizer orderByOptimizer = new OOrderByOptimizer();
  private final OMetricRecorder metricRecorder = new OMetricRecorder();
  private final OFilterOptimizer filterOptimizer = new OFilterOptimizer();
  private final OFilterAnalyzer filterAnalyzer = new OFilterAnalyzer();
  private Map<String, String> projectionDefinition = null;
  // THIS HAS BEEN KEPT FOR COMPATIBILITY; BUT IT'S USED THE PROJECTIONS IN GROUPED-RESULTS
  private Map<String, Object> projections = null;
  private List<OPair<String, String>> orderedFields = new ArrayList<OPair<String, String>>();
  private List<String> groupByFields;
  private ConcurrentHashMap<Object, ORuntimeResult> groupedResult =
      new ConcurrentHashMap<Object, ORuntimeResult>();
  private boolean aggregate = false;
  private List<String> unwindFields;
  private Object expandTarget;
  private int fetchLimit = -1;
  private OIdentifiable lastRecord;
  private String fetchPlan;
  private boolean fullySortedByIndex = false;
  private LOCKING_STRATEGY lockingStrategy = LOCKING_STRATEGY.DEFAULT;

  private Boolean isAnyFunctionAggregates = null;
  private volatile boolean parallel = false;
  private volatile boolean parallelRunning;
  private final ArrayBlockingQueue<AsyncResult> resultQueue;

  private ConcurrentHashMap<ORID, ORID> uniqueResult;
  private boolean noCache = false;
  private int tipLimitThreshold;

  private AtomicLong tmpQueueOffer = new AtomicLong();
  private Object resultLock = new Object();

  public OCommandExecutorSQLSelect() {
    OContextConfiguration conf = getDatabase().getConfiguration();
    resultQueue =
        new ArrayBlockingQueue<AsyncResult>(
            conf.getValueAsInteger(OGlobalConfiguration.QUERY_PARALLEL_RESULT_QUEUE_SIZE));
    tipLimitThreshold = conf.getValueAsInteger(OGlobalConfiguration.QUERY_LIMIT_THRESHOLD_TIP);
  }

  private static final class IndexUsageLog {
    private OIndex index;
    private List<Object> keyParams;
    private OIndexDefinition indexDefinition;

    IndexUsageLog(OIndex index, List<Object> keyParams, OIndexDefinition indexDefinition) {
      this.index = index;
      this.keyParams = keyParams;
      this.indexDefinition = indexDefinition;
    }
  }

  private final class IndexComparator implements Comparator<OIndex> {
    public int compare(final OIndex indexOne, final OIndex indexTwo) {
      final OIndexDefinition definitionOne = indexOne.getDefinition();
      final OIndexDefinition definitionTwo = indexTwo.getDefinition();

      final int firstParamCount = definitionOne.getParamCount();
      final int secondParamCount = definitionTwo.getParamCount();

      final int result = firstParamCount - secondParamCount;

      if (result == 0 && !orderedFields.isEmpty()) {
        if (!(indexOne instanceof OChainedIndexProxy)
            && orderByOptimizer.canBeUsedByOrderBy(
                indexOne, OCommandExecutorSQLSelect.this.orderedFields)) {
          return 1;
        }

        if (!(indexTwo instanceof OChainedIndexProxy)
            && orderByOptimizer.canBeUsedByOrderBy(
                indexTwo, OCommandExecutorSQLSelect.this.orderedFields)) {
          return -1;
        }
      }

      return result;
    }
  }

  private static Object getIndexKey(
      final OIndexDefinition indexDefinition, Object value, OCommandContext context) {
    if (indexDefinition instanceof OCompositeIndexDefinition
        || indexDefinition.getParamCount() > 1) {
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
      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        return ((OIndexDefinitionMultiValue) indexDefinition)
            .createSingleValue(OSQLHelper.getValue(value));
      else return indexDefinition.createValue(OSQLHelper.getValue(value, null, context));
    }
  }

  public boolean hasGroupBy() {
    return groupByFields != null && groupByFields.size() > 0;
  }

  @Override
  protected boolean isUseCache() {
    return !noCache && request.isUseCache();
  }

  private static ODocument createIndexEntryAsDocument(
      final Object iKey, final OIdentifiable iValue) {
    final ODocument doc = new ODocument().setOrdered(true);
    doc.field("key", iKey);
    doc.field("rid", iValue);
    ORecordInternal.unsetDirty(doc);
    return doc;
  }

  /** Compile the filter conditions only the first time. */
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;
    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
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
        parsedTarget =
            OSQLEngine.getInstance()
                .parseTarget(
                    parserText.substring(parserGetCurrentPosition(), endPosition), getContext());
        parserSetCurrentPosition(
            parsedTarget.parserIsEnded()
                ? endPosition
                : parsedTarget.parserGetCurrentPosition() + parserGetCurrentPosition());
      } else {
        parserGoBack();
      }

      if (!parserIsEnded()) {
        parserSkipWhiteSpaces();

        while (!parserIsEnded()) {
          final String w = parserNextWord(true);

          if (!w.isEmpty()) {
            if (w.equals(KEYWORD_WHERE)) {
              compiledFilter =
                  OSQLEngine.getInstance()
                      .parseCondition(
                          parserText.substring(parserGetCurrentPosition(), endPosition),
                          getContext(),
                          KEYWORD_WHERE);
              optimize();
              if (compiledFilter.parserIsEnded()) {
                parserSetCurrentPosition(endPosition);
              } else {
                parserSetCurrentPosition(
                    compiledFilter.parserGetCurrentPosition() + parserGetCurrentPosition());
              }
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
              } else if (lock.equals("SHARED")) {
                lockingStrategy = LOCKING_STRATEGY.SHARED_LOCK;
              }
            } else if (w.equals(KEYWORD_PARALLEL)) {
              parallel = parseParallel(w);
            } else {
              if (preParsedStatement == null) {
                throwParsingException("Invalid keyword '" + w + "'");
              } // if the pre-parsed statement is OK, then you can go on with the rest, the SQL is
              // valid and this is probably a space in a backtick
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
          if (f.getFunction().aggregateResults()
              && this.groupByFields != null
              && this.groupByFields.size() > 0) {
            throwParsingException(
                "Aggregate function cannot be used in LET clause together with GROUP BY");
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
        final Set<String> clIds =
            ((OCommandExecutorSQLResultsetDelegate) parsedTarget.getTargetRecords())
                .getInvolvedClusters();
        for (String c : clIds) {
          // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
          if (checkClusterAccess(db, c)) {
            clusters.add(c);
          }
        }

      } else if (parsedTarget.getTargetRecords() != null) {
        // SINGLE RECORDS: BROWSE ALL (COULD BE EXPENSIVE).
        for (OIdentifiable identifiable : parsedTarget.getTargetRecords()) {
          final String c =
              db.getClusterNameById(identifiable.getIdentity().getClusterId())
                  .toLowerCase(Locale.ENGLISH);
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
    if (isAnyFunctionAggregates == null) {
      if (projections != null) {
        for (Entry<String, Object> p : projections.entrySet()) {
          if (p.getValue() instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) p.getValue()).aggregateResults()) {
            isAnyFunctionAggregates = true;
            break;
          }
        }
      }

      if (isAnyFunctionAggregates == null) isAnyFunctionAggregates = false;
    }
    return isAnyFunctionAggregates;
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
      handleGroupBy(context);
      applyOrderBy(true);

      subIterator = new ArrayList<OIdentifiable>((List<OIdentifiable>) getResult()).iterator();
      lastRecord = null;
      tempResult = null;
      groupedResult.clear();
      aggregate = false;
    } else {
      subIterator = (Iterator<OIdentifiable>) target;
    }

    return subIterator;
  }

  public Object execute(final Map<Object, Object> iArgs) {
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
      handleGroupBy(context);
      applyOrderBy(true);
      applyLimitAndSkip();
    }
    return getResult();
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
            "No source found in query: specify class, cluster(s), index or single record(s). Use "
                + getSyntax());
      }
    }
    return true;
  }

  protected boolean executeSearchRecord(
      final OIdentifiable id, final OCommandContext iContext, boolean callHooks) {
    if (id == null) return false;

    final ORID identity = id.getIdentity();

    if (uniqueResult != null) {
      if (uniqueResult.containsKey(identity)) return true;

      if (identity.isValid()) uniqueResult.put(identity, identity);
    }

    if (!checkInterruption()) return false;

    final LOCKING_STRATEGY contextLockingStrategy =
        iContext.getVariable("$locking") != null
            ? (LOCKING_STRATEGY) iContext.getVariable("$locking")
            : null;

    final LOCKING_STRATEGY localLockingStrategy =
        contextLockingStrategy != null ? contextLockingStrategy : lockingStrategy;

    if (localLockingStrategy != null
        && !(localLockingStrategy == LOCKING_STRATEGY.DEFAULT
            || localLockingStrategy == LOCKING_STRATEGY.NONE
            || localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK
            || localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK))
      throw new IllegalStateException("Unsupported locking strategy " + localLockingStrategy);

    if (localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK) {
      id.lock(false);

      if (id instanceof ORecord) {
        final ORecord record = (ORecord) id;
        record.reload(null, true, false);
      }

    } else if (localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK) {
      id.lock(true);

      if (id instanceof ORecord) {
        final ORecord record = (ORecord) id;
        record.reload(null, true, false);
      }
    }

    ORecord record = null;
    try {
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

      iContext.updateMetric("recordReads", +1);

      if (record == null)
        // SKIP IT
        return true;
      if (ORecordInternal.getRecordType(record) != ODocument.RECORD_TYPE && checkSkipBlob())
        // SKIP binary records in case of projection.
        return true;

      iContext.updateMetric("documentReads", +1);

      iContext.setVariable("current", record);

      if (filter(record, iContext)) {
        if (callHooks) {
          getDatabase().beforeReadOperations(record);
          getDatabase().afterReadOperations(record);
        }

        if (parallel) {
          try {
            applyGroupBy(record, iContext);
            resultQueue.put(new AsyncResult(record, iContext));
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            return false;
          }
          tmpQueueOffer.incrementAndGet();
        } else {
          applyGroupBy(record, iContext);

          if (!handleResult(record, iContext)) {
            // LIMIT REACHED
            return false;
          }
        }
      }
    } finally {

      if (localLockingStrategy != null && record != null && record.isLocked()) {
        // CONTEXT LOCK: lock must be released (no matter if filtered or not)
        if (localLockingStrategy == LOCKING_STRATEGY.EXCLUSIVE_LOCK
            || localLockingStrategy == LOCKING_STRATEGY.SHARED_LOCK) {
          record.unlock();
        }
      }
    }

    return true;
  }

  private boolean checkSkipBlob() {
    if (expandTarget != null) return true;
    if (projections != null) {
      if (projections.size() > 1) {
        return true;
      }
      if (projections.containsKey("@rid")) return false;
    }
    return false;
  }

  /**
   * Handles the record in result.
   *
   * @param iRecord Record to handle
   * @return false if limit has been reached, otherwise true
   */
  @Override
  protected boolean handleResult(final OIdentifiable iRecord, final OCommandContext iContext) {
    lastRecord = iRecord;

    if ((orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort())
        && skip > 0
        && this.unwindFields == null
        && this.expandTarget == null) {
      lastRecord = null;
      skip--;
      return true;
    }

    if (!addResult(lastRecord, iContext)) {
      return false;
    }

    return continueSearching();
  }

  private boolean continueSearching() {
    return !((orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort())
        && !isAnyFunctionAggregates()
        && (groupByFields == null || groupByFields.isEmpty())
        && fetchLimit > -1
        && resultCount >= fetchLimit
        && expandTarget == null);
  }

  /**
   * Returns the temporary RID counter assuring it's unique per query tree.
   *
   * @return Serial as integer
   */
  public int getTemporaryRIDCounter(final OCommandContext iContext) {
    final OTemporaryRidGenerator parentQuery =
        (OTemporaryRidGenerator) iContext.getVariable("parentQuery");
    if (parentQuery != null && parentQuery != this) {
      return parentQuery.getTemporaryRIDCounter(iContext);
    } else {
      return serialTempRID.getAndIncrement();
    }
  }

  protected boolean addResult(OIdentifiable iRecord, final OCommandContext iContext) {
    resultCount++;
    if (iRecord == null) return true;

    if (projections != null || groupByFields != null && !groupByFields.isEmpty()) {
      if (!aggregate) {
        // APPLY PROJECTIONS IN LINE

        iRecord =
            ORuntimeResult.getProjectionResult(
                getTemporaryRIDCounter(iContext), projections, iContext, iRecord);
        if (iRecord == null) {
          resultCount--; // record discarded
          return true;
        }

      } else {
        // GROUP BY
        return true;
      }
    }

    if (tipLimitThreshold > 0 && resultCount > tipLimitThreshold && getLimit() == -1) {
      reportTip(
          String.format(
              "Query '%s' returned a result set with more than %d records. Check if you really need all these records, or reduce the resultset by using a LIMIT to improve both performance and used RAM",
              parserText, tipLimitThreshold));
      tipLimitThreshold = 0;
    }

    List<OIdentifiable> allResults = new ArrayList<OIdentifiable>();
    if (unwindFields != null) {
      Collection<OIdentifiable> partial = unwind(iRecord, this.unwindFields, iContext);

      for (OIdentifiable item : partial) {
        allResults.add(item);
      }
    } else {
      allResults.add(iRecord);
    }
    boolean result = true;
    if (allowsStreamedResult()) {
      // SEND THE RESULT INLINE
      if (request.getResultListener() != null)
        for (OIdentifiable iRes : allResults) {
          result = pushResult(iRes);
        }
    } else {

      // COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
      if (tempResult == null) tempResult = new ArrayList<OIdentifiable>();

      applyPartialOrderBy();

      for (OIdentifiable iRes : allResults) {
        ((Collection<OIdentifiable>) tempResult).add(iRes);
      }
    }

    return result;
  }

  private ODocument applyGroupBy(final OIdentifiable iRecord, final OCommandContext iContext) {
    if (!aggregate) return null;

    // AGGREGATION/GROUP BY
    Object fieldValue = null;
    if (groupByFields != null && !groupByFields.isEmpty()) {
      if (groupByFields.size() > 1) {
        // MULTI-FIELD GROUP BY
        final ODocument doc = iRecord.getRecord();
        final Object[] fields = new Object[groupByFields.size()];
        for (int i = 0; i < groupByFields.size(); ++i) {
          final String field = groupByFields.get(i);
          if (field.startsWith("$")) fields[i] = iContext.getVariable(field);
          else fields[i] = doc.field(field);
        }
        fieldValue = fields;
      } else {
        final String field = groupByFields.get(0);
        if (field != null) {
          if (field.startsWith("$")) fieldValue = iContext.getVariable(field);
          else fieldValue = ((ODocument) iRecord.getRecord()).field(field);
        }
      }
    }

    return getProjectionGroup(fieldValue, iContext).applyRecord(iRecord);
  }

  private boolean allowsStreamedResult() {
    return (fullySortedByIndex || orderedFields.isEmpty())
        && expandTarget == null
        && unwindFields == null;
  }

  /**
   * in case of ORDER BY + SKIP + LIMIT, this method applies ORDER BY operation on partial result
   * and discards overflowing results (results > skip + limit)
   */
  private void applyPartialOrderBy() {
    if (expandTarget != null
        || (unwindFields != null && unwindFields.size() > 0)
        || orderedFields.isEmpty()
        || fullySortedByIndex
        || isRidOnlySort()) {
      return;
    }

    if (limit > 0) {
      int sortBufferSize = limit + 1;
      if (skip > 0) {
        sortBufferSize += skip;
      }
      if (tempResult instanceof List
          && ((List) tempResult).size() >= sortBufferSize + PARTIAL_SORT_BUFFER_THRESHOLD) {
        applyOrderBy(false);
        tempResult = new ArrayList(((List) tempResult).subList(0, sortBufferSize));
      }
    }
  }

  private Collection<OIdentifiable> unwind(
      final OIdentifiable iRecord,
      final List<String> unwindFields,
      final OCommandContext iContext) {
    final List<OIdentifiable> result = new ArrayList<OIdentifiable>();
    ODocument doc;
    if (iRecord instanceof ODocument) {
      doc = (ODocument) iRecord;
    } else {
      doc = iRecord.getRecord();
    }
    if (unwindFields.size() == 0) {
      ORecordInternal.setIdentity(doc, new ORecordId(-2, getTemporaryRIDCounter(iContext)));
      result.add(doc);
    } else {
      String firstField = unwindFields.get(0);
      final List<String> nextFields = unwindFields.subList(1, unwindFields.size());

      Object fieldValue = doc.field(firstField);
      if (fieldValue == null
          || !(fieldValue instanceof Iterable)
          || fieldValue instanceof ODocument) {
        result.addAll(unwind(doc, nextFields, iContext));
      } else {
        Iterator iterator = ((Iterable) fieldValue).iterator();
        if (!iterator.hasNext()) {
          ODocument unwindedDoc = new ODocument();
          doc.copyTo(unwindedDoc);
          unwindedDoc.field(firstField, (Object) null);
          result.addAll(unwind(unwindedDoc, nextFields, iContext));
        } else {
          do {
            Object o = iterator.next();
            ODocument unwindedDoc = new ODocument();
            doc.copyTo(unwindedDoc);
            unwindedDoc.field(firstField, o);
            result.addAll(unwind(unwindedDoc, nextFields, iContext));
          } while (iterator.hasNext());
        }
      }
    }
    return result;
  }

  /**
   * Report the tip to the profiler and collect it in context to be reported by tools like Studio
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

  protected ORuntimeResult getProjectionGroup(
      final Object fieldValue, final OCommandContext iContext) {
    final long projectionElapsed = (Long) context.getVariable("projectionElapsed", 0l);
    final long begin = System.currentTimeMillis();
    try {

      aggregate = true;

      Object key;

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
              keyArray.append(
                  o instanceof OIdentifiable
                      ? ((OIdentifiable) o).getIdentity().toString()
                      : o.toString());
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
        group =
            new ORuntimeResult(
                fieldValue,
                createProjectionFromDefinition(),
                getTemporaryRIDCounter(iContext),
                context);
        final ORuntimeResult prev = groupedResult.putIfAbsent(key, group);
        if (prev != null)
          // ALREADY EXISTENT: USE THIS
          group = prev;
      }
      return group;

    } finally {
      context.setVariable(
          "projectionElapsed", projectionElapsed + (System.currentTimeMillis() - begin));
    }
  }

  protected void parseGroupBy() {
    parserRequiredKeyword(KEYWORD_BY);

    groupByFields = new ArrayList<String>();
    while (!parserIsEnded()
        && (groupByFields.size() == 0
            || parserGetLastSeparator() == ','
            || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");
      groupByFields.add(fieldName);
      parserSkipWhiteSpaces();
    }

    if (groupByFields.size() == 0) {
      throwParsingException("Group by field set was missed. Example: GROUP BY name, salary");
    }

    // AGGREGATE IT
    aggregate = true;
    groupedResult.clear();
  }

  protected void parseUnwind() {
    unwindFields = new ArrayList<String>();
    while (!parserIsEnded()
        && (unwindFields.size() == 0
            || parserGetLastSeparator() == ','
            || parserGetCurrentChar() == ',')) {
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
    while (!parserIsEnded()
        && (orderedFields.size() == 0
            || parserGetLastSeparator() == ','
            || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");

      parserOptionalWord(true);

      final String word = parserGetLastWord();

      if (word.length() == 0)
      // END CLAUSE: SET AS ASC BY DEFAULT
      {
        fieldOrdering = KEYWORD_ASC;
      } else if (word.equals(KEYWORD_LIMIT)
          || word.equals(KEYWORD_SKIP)
          || word.equals(KEYWORD_OFFSET)) {
        // NEXT CLAUSE: SET AS ASC BY DEFAULT
        fieldOrdering = KEYWORD_ASC;
        parserGoBack();
      } else {
        if (word.equals(KEYWORD_ASC)) {
          fieldOrdering = KEYWORD_ASC;
        } else if (word.equals(KEYWORD_DESC)) {
          fieldOrdering = KEYWORD_DESC;
        } else {
          throwParsingException(
              "Ordering mode '"
                  + word
                  + "' not supported. Valid is 'ASC', 'DESC' or nothing ('ASC' by default)");
        }
      }

      orderedFields.add(new OPair<String, String>(fieldName, fieldOrdering));
      parserSkipWhiteSpaces();
    }

    if (orderedFields.size() == 0) {
      throwParsingException(
          "Order by field set was missed. Example: ORDER BY name ASC, salary DESC");
    }
  }

  @Override
  protected void searchInClasses() {
    final String className = parsedTarget.getTargetClasses().keySet().iterator().next();

    final OClass cls = getDatabase().getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (!searchForIndexes(cls) && !searchForSubclassIndexes(cls)) {
      // CHECK FOR INVERSE ORDER
      final boolean browsingOrderAsc = isBrowsingAscendingOrder();
      super.searchInClasses(browsingOrderAsc);
    }
  }

  private boolean isBrowsingAscendingOrder() {
    return !(orderedFields.size() == 1
        && orderedFields.get(0).getKey().equalsIgnoreCase("@rid")
        && orderedFields.get(0).getValue().equalsIgnoreCase("DESC"));
  }

  protected int parseProjections() {
    if (!parserOptionalKeyword(KEYWORD_SELECT)) return -1;

    int upperBound =
        OStringSerializerHelper.getLowerIndexOfKeywords(
            parserTextUpperCase, parserGetCurrentPosition(), KEYWORD_FROM, KEYWORD_LET);
    if (upperBound == -1)
      // UP TO THE END
      upperBound = parserText.length();

    int lastRealPositionProjection = -1;

    int currPos = parserGetCurrentPosition();
    if (currPos == -1) return -1;

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
          throw new OCommandSQLParsingException(
              "Projection not allowed with FLATTEN() and EXPAND() operators");

        final List<String> words = OStringSerializerHelper.smartSplit(projection, ' ');

        String fieldName;
        if (words.size() > 1 && words.get(1).trim().equalsIgnoreCase(KEYWORD_AS)) {
          // FOUND AS, EXTRACT ALIAS
          if (words.size() < 3) throw new OCommandSQLParsingException("Found 'AS' without alias");

          fieldName = words.get(2).trim();

          if (projectionDefinition.containsKey(fieldName))
            throw new OCommandSQLParsingException(
                "Field '"
                    + fieldName
                    + "' is duplicated in current SELECT, choose a different name");

          projection = words.get(0).trim();

          if (words.size() > 3) lastRealPositionProjection = projectionString.indexOf(words.get(3));
          else lastRealPositionProjection += projectionItem.length() + 1;

        } else {
          // EXTRACT THE FIELD NAME WITHOUT FUNCTIONS AND/OR LINKS
          projection = words.get(0);
          fieldName = projection;

          lastRealPositionProjection = projectionString.indexOf(fieldName) + fieldName.length() + 1;

          if (fieldName.charAt(0) == '@') fieldName = fieldName.substring(1);

          endPos = extractProjectionNameSubstringEndPosition(fieldName);

          if (endPos > -1) fieldName = fieldName.substring(0, endPos);

          // FIND A UNIQUE NAME BY ADDING A COUNTER
          for (int fieldIndex = 2; projectionDefinition.containsKey(fieldName); ++fieldIndex)
            fieldName += fieldIndex;
        }

        final String p = OSQLPredicate.upperCase(projection);
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

          if (!aggregate
              && expandTarget instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) expandTarget).aggregateResults()) aggregate = true;

          continue;
        }

        fieldName = OIOUtils.getStringContent(fieldName);

        projectionDefinition.put(fieldName, projection);
      }

      if (projectionDefinition != null
          && (projectionDefinition.size() > 1
              || !projectionDefinition.values().iterator().next().equals("*"))) {
        projections = createProjectionFromDefinition();

        for (Object p : projections.values()) {

          if (!aggregate
              && p instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) p).aggregateResults()) {
            // AGGREGATE IT
            getProjectionGroup(null, context);
            break;
          }
        }

      } else {
        // TREATS SELECT * AS NO PROJECTION
        projectionDefinition = null;
        projections = null;
      }
    }

    if (upperBound < parserText.length() - 1) parserSetCurrentPosition(upperBound);
    else if (lastRealPositionProjection > -1) parserMoveCurrentPosition(lastRealPositionProjection);
    else parserSetEndOfText();

    return parserGetCurrentPosition();
  }

  protected Map<String, Object> createProjectionFromDefinition() {
    if (projectionDefinition == null) {
      return new LinkedHashMap<String, Object>();
    }

    final Map<String, Object> projections =
        new LinkedHashMap<String, Object>(projectionDefinition.size());
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

  /** Parses the fetchplan keyword if found. */
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
      final String word = OIOUtils.getStringContent(parserNextWord(true));
      if (!OPatternConst.PATTERN_FETCH_PLAN.matcher(word).matches()) {
        break;
      }

      end = parserGetCurrentPosition();
      parserSkipWhiteSpaces();
      position = parserGetCurrentPosition();
    }

    parserSetCurrentPosition(position);

    if (end < 0) {
      fetchPlan = OIOUtils.getStringContent(parserText.substring(start));
    } else {
      fetchPlan = OIOUtils.getStringContent(parserText.substring(start, end));
    }

    request.setFetchPlan(fetchPlan);

    return true;
  }

  protected boolean optimizeExecution() {
    if (compiledFilter != null) {
      mergeRangeConditionsToBetweenOperators(compiledFilter);
    }

    if ((compiledFilter == null || (compiledFilter.getRootCondition() == null))
        && groupByFields == null
        && projections != null
        && projections.size() == 1) {

      final long startOptimization = System.currentTimeMillis();
      try {

        final Entry<String, Object> entry = projections.entrySet().iterator().next();

        if (entry.getValue() instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
          if (rf.function instanceof OSQLFunctionCount
              && rf.configuredParameters.length == 1
              && "*".equals(rf.configuredParameters[0])) {

            final boolean restrictedClasses = isUsingRestrictedClasses();

            if (!restrictedClasses) {
              long count = 0;

              final ODatabaseDocumentInternal database = getDatabase();
              if (parsedTarget.getTargetClasses() != null) {
                final String className = parsedTarget.getTargetClasses().keySet().iterator().next();
                final OClass cls =
                    database.getMetadata().getImmutableSchemaSnapshot().getClass(className);
                count = cls.count();
              } else if (parsedTarget.getTargetClusters() != null) {
                for (String cluster : parsedTarget.getTargetClusters().keySet()) {
                  count += database.countClusterElements(cluster);
                }
              } else if (parsedTarget.getTargetIndex() != null) {
                count +=
                    database
                        .getMetadata()
                        .getIndexManagerInternal()
                        .getIndex(database, parsedTarget.getTargetIndex())
                        .getInternal()
                        .size();
              } else {
                final Iterable<? extends OIdentifiable> recs = parsedTarget.getTargetRecords();
                if (recs != null) {
                  if (recs instanceof Collection<?>) count += ((Collection<?>) recs).size();
                  else {
                    for (Object o : recs) count++;
                  }
                }
              }

              if (tempResult == null) tempResult = new ArrayList<OIdentifiable>();
              ((Collection<OIdentifiable>) tempResult)
                  .add(new ODocument().field(entry.getKey(), count));
              return true;
            }
          }
        }

      } finally {
        context.setVariable(
            "optimizationElapsed", (System.currentTimeMillis() - startOptimization));
      }
    }

    return false;
  }

  private boolean isUsingRestrictedClasses() {
    boolean restrictedClasses = false;
    final OSecurityUser user = getDatabase().getUser();

    if (parsedTarget.getTargetClasses() != null
        && user != null
        && user.checkIfAllowed(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ)
            == null) {
      for (String className : parsedTarget.getTargetClasses().keySet()) {
        final OClass cls =
            getDatabase().getMetadata().getImmutableSchemaSnapshot().getClass(className);
        if (cls.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
          restrictedClasses = true;
          break;
        }
      }
    }
    return restrictedClasses;
  }

  protected void revertSubclassesProfiler(final OCommandContext iContext, int num) {
    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      profiler.updateCounter(
          profiler.getDatabaseMetric(getDatabase().getName(), "query.indexUseAttemptedAndReverted"),
          "Reverted index usage in query",
          num);
    }
  }

  protected void revertProfiler(
      final OCommandContext iContext,
      final OIndex index,
      final List<Object> keyParams,
      final OIndexDefinition indexDefinition) {
    if (iContext.isRecordingMetrics()) {
      iContext.updateMetric("compositeIndexUsed", -1);
    }

    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      profiler.updateCounter(
          profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"),
          "Used index in query",
          -1);

      int params = indexDefinition.getParamCount();
      if (params > 1) {
        final String profiler_prefix =
            profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");

        profiler.updateCounter(profiler_prefix, "Used composite index in query", -1);
        profiler.updateCounter(
            profiler_prefix + "." + params,
            "Used composite index in query with " + params + " params",
            -1);
        profiler.updateCounter(
            profiler_prefix + "." + params + '.' + keyParams.size(),
            "Used composite index in query with "
                + params
                + " params and "
                + keyParams.size()
                + " keys",
            -1);
      }
    }
  }

  /** Parses the NOCACHE keyword if found. */
  protected boolean parseNoCache(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_NOCACHE)) return false;

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

  private OSQLFilterCondition convertToBetweenClause(final OSQLFilterCondition condition) {
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

    if (rightCondition.getLeft() instanceof OSQLFilterItemField
        && rightCondition.getRight() instanceof OSQLFilterItemField) {
      return null;
    }

    if (!(rightCondition.getLeft() instanceof OSQLFilterItemField)
        && !(rightCondition.getRight() instanceof OSQLFilterItemField)) {
      return null;
    }

    if (leftCondition.getLeft() instanceof OSQLFilterItemField
        && leftCondition.getRight() instanceof OSQLFilterItemField) {
      return null;
    }

    if (!(leftCondition.getLeft() instanceof OSQLFilterItemField)
        && !(leftCondition.getRight() instanceof OSQLFilterItemField)) {
      return null;
    }

    final List<Object> betweenBoundaries = new ArrayList<Object>();

    if (rightCondition.getLeft() instanceof OSQLFilterItemField) {
      final OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getLeft();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      rightField = itemField.getRoot();
      betweenBoundaries.add(rightCondition.getRight());
    } else if (rightCondition.getRight() instanceof OSQLFilterItemField) {
      final OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getRight();
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
      final OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getLeft();
      if (!itemField.isFieldChain()) {
        return null;
      }

      if (itemField.getFieldChain().getItemCount() > 1) {
        return null;
      }

      leftField = itemField.getRoot();
      betweenBoundaries.add(leftCondition.getRight());
    } else if (leftCondition.getRight() instanceof OSQLFilterItemField) {
      final OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getRight();
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

    if ((rightOperator instanceof OQueryOperatorMajor
            || rightOperator instanceof OQueryOperatorMajorEquals)
        && (leftOperator instanceof OQueryOperatorMinor
            || leftOperator instanceof OQueryOperatorMinorEquals)) {

      final OQueryOperatorBetween between = new OQueryOperatorBetween();

      if (rightOperator instanceof OQueryOperatorMajor) {
        between.setLeftInclusive(false);
      }

      if (leftOperator instanceof OQueryOperatorMinor) {
        between.setRightInclusive(false);
      }

      return new OSQLFilterCondition(
          new OSQLFilterItemField(this, leftField, null), between, betweenBoundaries.toArray());
    }

    if ((leftOperator instanceof OQueryOperatorMajor
            || leftOperator instanceof OQueryOperatorMajorEquals)
        && (rightOperator instanceof OQueryOperatorMinor
            || rightOperator instanceof OQueryOperatorMinorEquals)) {
      final OQueryOperatorBetween between = new OQueryOperatorBetween();

      if (leftOperator instanceof OQueryOperatorMajor) {
        between.setLeftInclusive(false);
      }

      if (rightOperator instanceof OQueryOperatorMinor) {
        between.setRightInclusive(false);
      }

      Collections.reverse(betweenBoundaries);

      return new OSQLFilterCondition(
          new OSQLFilterItemField(this, leftField, null), between, betweenBoundaries.toArray());
    }

    return null;
  }

  public void initContext() {
    if (context == null) {
      context = new OBasicCommandContext();
    }
    if (context.getDatabase() == null) {
      ((OBasicCommandContext) context).setDatabase(getDatabase());
    }

    metricRecorder.setContext(context);
  }

  private boolean fetchFromTarget(final Iterator<? extends OIdentifiable> iTarget) {
    fetchLimit = getQueryFetchLimit();

    final long startFetching = System.currentTimeMillis();

    final int[] clusterIds;
    if (iTarget instanceof ORecordIteratorClusters) {
      clusterIds = ((ORecordIteratorClusters) iTarget).getClusterIds();
    } else {
      clusterIds = null;
    }

    parallel =
        (parallel
                || getDatabase()
                    .getConfiguration()
                    .getValueAsBoolean(OGlobalConfiguration.QUERY_PARALLEL_AUTO))
            && canRunParallel(clusterIds, iTarget);

    try {
      if (parallel) return parallelExec(iTarget);

      boolean prefetchPages = false;
      if (canScanStorageCluster(clusterIds)) prefetchPages = true;

      // WORK WITH ITERATOR
      ODatabaseDocumentInternal database = getDatabase();
      database.setPrefetchRecords(prefetchPages);
      try {
        return serialIterator(iTarget);
      } finally {
        database.setPrefetchRecords(false);
      }

    } finally {
      context.setVariable(
          "fetchingFromTargetElapsed", (System.currentTimeMillis() - startFetching));
    }
  }

  private boolean canRunParallel(int[] clusterIds, Iterator<? extends OIdentifiable> iTarget) {
    if (getDatabase().getTransaction().isActive()) return false;

    if (iTarget instanceof ORecordIteratorClusters) {
      if (clusterIds.length > 1) {
        final long totalRecords = getDatabase().countClusterElements(clusterIds);
        if (totalRecords
            > getDatabase()
                .getConfiguration()
                .getValueAsLong(OGlobalConfiguration.QUERY_PARALLEL_MINIMUM_RECORDS)) {
          // ACTIVATE PARALLEL
          OLogManager.instance()
              .debug(
                  this,
                  "Activated parallel query. clusterIds=%d, totalRecords=%d",
                  clusterIds.length,
                  totalRecords);
          return true;
        }
      }
    }
    return false;
  }

  private boolean canScanStorageCluster(final int[] clusterIds) {
    final ODatabaseDocumentInternal db = getDatabase();

    if (clusterIds != null && request.isIdempotent() && !db.getTransaction().isActive()) {
      final OImmutableSchema schema =
          ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot();
      for (int clusterId : clusterIds) {
        final OImmutableClass cls = (OImmutableClass) schema.getClassByClusterId(clusterId);
        if (cls != null) {
          if (cls.isRestricted() || cls.isOuser() || cls.isOrole()) return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean serialIterator(Iterator<? extends OIdentifiable> iTarget) {
    int queryScanThresholdWarning =
        getDatabase()
            .getConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_SCAN_THRESHOLD_TIP);

    boolean tipActivated =
        queryScanThresholdWarning > 0
            && iTarget instanceof OIdentifiableIterator
            && compiledFilter != null;

    // BROWSE, UNMARSHALL AND FILTER ALL THE RECORDS ON CURRENT THREAD
    for (int browsed = 0; iTarget.hasNext(); browsed++) {
      final OIdentifiable next = iTarget.next();
      if (!executeSearchRecord(next, context, false)) return false;
    }
    return true;
  }

  private boolean parseParallel(String w) {
    return w.equals(KEYWORD_PARALLEL);
  }

  private boolean parallelExec(final Iterator<? extends OIdentifiable> iTarget) {
    final OLegacyResultSet result = (OLegacyResultSet) getResultInstance();

    // BROWSE ALL THE RECORDS ON CURRENT THREAD BUT DELEGATE UNMARSHALLING AND FILTER TO A THREAD
    // POOL
    final ODatabaseDocumentInternal db = getDatabase();

    if (limit > -1) {
      if (result != null) {
        result.setLimit(limit);
      }
    }

    final boolean res = execParallelWithPool((ORecordIteratorClusters) iTarget, db);

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "Parallel query '%s' completed", parserText);

    return res;
  }

  private boolean execParallelWithPool(
      final ORecordIteratorClusters iTarget, final ODatabaseDocumentInternal db) {
    final int[] clusterIds = iTarget.getClusterIds();

    // CREATE ONE THREAD PER CLUSTER
    final int jobNumbers = clusterIds.length;
    final List<Future<?>> jobs = new ArrayList<Future<?>>();

    OLogManager.instance()
        .debug(
            this,
            "Executing parallel query with strategy executors. clusterIds=%d, jobs=%d",
            clusterIds.length,
            jobNumbers);

    final boolean[] results = new boolean[jobNumbers];
    final OCommandContext[] contexts = new OCommandContext[jobNumbers];

    final RuntimeException[] exceptions = new RuntimeException[jobNumbers];

    parallelRunning = true;

    final AtomicInteger runningJobs = new AtomicInteger(jobNumbers);

    for (int i = 0; i < jobNumbers; ++i) {
      final int current = i;

      final Runnable job =
          () -> {
            try {
              ODatabaseDocumentInternal localDatabase = null;
              try {
                exceptions[current] = null;
                results[current] = true;

                final OCommandContext threadContext = context.copy();
                contexts[current] = threadContext;

                localDatabase = db.copy();
                localDatabase.activateOnCurrentThread();

                // CREATE A SNAPSHOT TO AVOID DEADLOCKS
                db.getMetadata().getSchema().makeSnapshot();

                scanClusterWithIterator(
                    localDatabase, threadContext, clusterIds[current], current, results);

              } catch (RuntimeException t) {
                exceptions[current] = t;
              } finally {
                runningJobs.decrementAndGet();
                resultQueue.offer(PARALLEL_END_EXECUTION_THREAD);

                if (localDatabase != null) localDatabase.close();
              }
            } catch (Exception e) {
              if (exceptions[current] == null) {
                exceptions[current] = new RuntimeException(e);
              }

              OLogManager.instance().error(this, "Error during command execution", e);
            }
            ODatabaseRecordThreadLocal.instance().remove();
          };

      jobs.add(Orient.instance().submit(job));
    }

    final int maxQueueSize =
        getDatabase()
                .getConfiguration()
                .getValueAsInteger(OGlobalConfiguration.QUERY_PARALLEL_RESULT_QUEUE_SIZE)
            - 1;

    boolean cancelQuery = false;
    boolean tipProvided = false;
    while (runningJobs.get() > 0 || !resultQueue.isEmpty()) {
      try {
        final AsyncResult result = resultQueue.take();

        final int qSize = resultQueue.size();

        if (!tipProvided && qSize >= maxQueueSize) {
          OLogManager.instance()
              .debug(
                  this,
                  "Parallel query '%s' has result queue full (size=%d), this could reduce concurrency level. Consider increasing queue size with setting: %s=<size>",
                  parserText,
                  maxQueueSize + 1,
                  OGlobalConfiguration.QUERY_PARALLEL_RESULT_QUEUE_SIZE.getKey());
          tipProvided = true;
        }

        if (OExecutionThreadLocal.isInterruptCurrentOperation())
          throw new InterruptedException("Operation has been interrupted");

        if (result != PARALLEL_END_EXECUTION_THREAD) {

          if (!handleResult(result.record, result.context)) {
            // STOP EXECUTORS
            parallelRunning = false;
            break;
          }
        }

      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        cancelQuery = true;
        break;
      }
    }

    parallelRunning = false;

    if (cancelQuery) {
      // CANCEL ALL THE RUNNING JOBS
      for (int i = 0; i < jobs.size(); ++i) {
        jobs.get(i).cancel(true);
      }
    } else {
      // JOIN ALL THE JOBS
      for (int i = 0; i < jobs.size(); ++i) {
        try {
          jobs.get(i).get();
          context.merge(contexts[i]);
        } catch (InterruptedException ignore) {
          break;
        } catch (final ExecutionException e) {
          OLogManager.instance().error(this, "Error on executing parallel query", e);
          throw OException.wrapException(
              new OCommandExecutionException("Error on executing parallel query"), e);
        }
      }
    }

    // CHECK FOR ANY EXCEPTION
    for (int i = 0; i < jobNumbers; ++i) if (exceptions[i] != null) throw exceptions[i];

    for (int i = 0; i < jobNumbers; ++i) {
      if (!results[i]) return false;
    }
    return true;
  }

  private void scanClusterWithIterator(
      final ODatabaseDocumentInternal localDatabase,
      final OCommandContext iContext,
      final int iClusterId,
      final int current,
      final boolean[] results) {
    final ORecordIteratorCluster it = new ORecordIteratorCluster(localDatabase, iClusterId);

    while (it.hasNext()) {
      final ORecord next = it.next();

      if (!executeSearchRecord(next, iContext, false)) {
        results[current] = false;
        break;
      }

      if (parallel && !parallelRunning)
        // EXECUTION ENDED
        break;
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

  private Stream<ORawPair<Object, ORID>> tryGetOptimizedSortStream(final OClass iSchemaClass) {
    if (orderedFields.size() == 0) {
      return null;
    } else {
      return getOptimizedSortStream(iSchemaClass);
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

    final OOrderBy order = new OOrderBy();
    order.setItems(new ArrayList<OOrderByItem>());
    if (this.orderedFields != null) {
      for (OPair<String, String> pair : this.orderedFields) {
        OOrderByItem item = new OOrderByItem();
        item.setRecordAttr(pair.getKey());
        if (pair.getValue() == null) {
          item.setType(OOrderByItem.ASC);
        } else {
          item.setType(
              pair.getValue().toUpperCase(Locale.ENGLISH).equals("DESC")
                  ? OOrderByItem.DESC
                  : OOrderByItem.ASC);
        }
        order.getItems().add(item);
      }
    }
    OSortedMultiIterator<OIdentifiable> cursor = new OSortedMultiIterator<>(order);
    boolean fullySorted = true;

    if (!iSchemaClass.isAbstract()) {
      Iterator<OIdentifiable> parentClassIterator =
          (Iterator<OIdentifiable>) searchInClasses(iSchemaClass, false, true);
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
      List<Stream<ORawPair<Object, ORID>>> substreams = getIndexCursors(subclass);
      fullySorted = fullySorted && fullySortedByIndex;
      if (substreams == null || substreams.size() == 0) {
        if (attempted > 0) {
          revertSubclassesProfiler(context, attempted);
        }
        return false;
      }
      for (Stream<ORawPair<Object, ORID>> c : substreams) {
        if (!fullySortedByIndex) {
          // TODO sort every iterator
        }
        attempted++;
        cursor.add(c.map((pair) -> (OIdentifiable) pair.second).iterator());
      }
    }
    fullySortedByIndex = fullySorted;

    uniqueResult = new ConcurrentHashMap<ORID, ORID>();

    fetchFromTarget(cursor);

    if (uniqueResult != null) {
      uniqueResult.clear();
    }
    uniqueResult = null;

    return true;
  }

  @SuppressWarnings("rawtypes")
  private List<Stream<ORawPair<Object, ORID>>> getIndexCursors(final OClass iSchemaClass) {

    final ODatabaseDocument database = getDatabase();

    // Leaving this in for reference, for the moment.
    // This should not be necessary as searchInClasses() does a security check and when the record
    // iterator
    // calls OClassImpl.readableClusters(), it too filters out clusters based on the class's
    // security permissions.
    // This throws an unnecessary exception that potentially prevents using an index and prevents
    // filtering later.
    //    database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ,
    // iSchemaClass.getName().toLowerCase(Locale.ENGLISH));

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null) {
      Stream<ORawPair<Object, ORID>> stream = tryGetOptimizedSortStream(iSchemaClass);
      if (stream == null) {
        return null;
      }
      List<Stream<ORawPair<Object, ORID>>> result = new ArrayList<>();
      result.add(stream);
      return result;
    }

    // the main condition is a set of sub-conditions separated by OR operators
    final List<List<OIndexSearchResult>> conditionHierarchy =
        filterAnalyzer.analyzeMainCondition(
            compiledFilter.getRootCondition(), iSchemaClass, context);
    if (conditionHierarchy == null) return null;

    List<Stream<ORawPair<Object, ORID>>> cursors = new ArrayList<>();

    boolean indexIsUsedInOrderBy = false;
    List<IndexUsageLog> indexUseAttempts = new ArrayList<IndexUsageLog>();
    // try {

    OIndexSearchResult lastSearchResult = null;
    for (List<OIndexSearchResult> indexSearchResults : conditionHierarchy) {
      // go through all variants to choose which one can be used for index search.
      boolean indexUsed = false;
      for (final OIndexSearchResult searchResult : indexSearchResults) {
        lastSearchResult = searchResult;
        final List<OIndex> involvedIndexes =
            filterAnalyzer.getInvolvedIndexes(iSchemaClass, searchResult);

        Collections.sort(involvedIndexes, new IndexComparator());

        // go through all possible index for given set of fields.
        for (final OIndex index : involvedIndexes) {

          final OIndexDefinition indexDefinition = index.getDefinition();

          if (searchResult.containsNullValues && indexDefinition.isNullValuesIgnored()) {
            continue;
          }

          final OQueryOperator operator = searchResult.lastOperator;

          // we need to test that last field in query subset and field in index that has the same
          // position
          // are equals.
          if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
            final String lastFiled =
                searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
            final String relatedIndexField =
                indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
            if (!lastFiled.equals(relatedIndexField)) {
              continue;
            }
          }

          final int searchResultFieldsCount = searchResult.fields().size();
          final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
          // We get only subset contained in processed sub query.
          for (final String fieldName :
              indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
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

          Stream<ORawPair<Object, ORID>> cursor;
          indexIsUsedInOrderBy =
              orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)
                  && !(index.getInternal() instanceof OChainedIndexProxy);
          try {
            boolean ascSortOrder =
                !indexIsUsedInOrderBy || orderedFields.get(0).getValue().equals(KEYWORD_ASC);

            if (indexIsUsedInOrderBy) {
              fullySortedByIndex =
                  expandTarget == null
                      && indexDefinition.getFields().size() >= orderedFields.size()
                      && conditionHierarchy.size() == 1;
            }

            context.setVariable("$limit", limit);

            cursor = operator.executeIndexQuery(context, index, keyParams, ascSortOrder);

          } catch (OIndexEngineException e) {
            throw e;
          } catch (Exception e) {
            OLogManager.instance()
                .error(
                    this,
                    "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                    e,
                    index.getName(),
                    request != null && request.getText() != null ? request.getText() : "");

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
        Stream<ORawPair<Object, ORID>> stream = tryGetOptimizedSortStream(iSchemaClass);
        if (stream == null) {
          return null;
        }
        List<Stream<ORawPair<Object, ORID>>> result = new ArrayList<>();
        result.add(stream);
        return result;
      }
    }

    if (cursors.size() == 0 || lastSearchResult == null) {
      return null;
    }

    metricRecorder.recordOrderByOptimizationMetric(indexIsUsedInOrderBy, this.fullySortedByIndex);

    indexUseAttempts.clear();

    return cursors;
  }

  @SuppressWarnings("rawtypes")
  private boolean searchForIndexes(final OClass iSchemaClass) {
    if (uniqueResult != null) uniqueResult.clear();

    final ODatabaseDocument database = getDatabase();
    database.checkSecurity(
        ORule.ResourceGeneric.CLASS,
        ORole.PERMISSION_READ,
        iSchemaClass.getName().toLowerCase(Locale.ENGLISH));

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null) {
      return tryOptimizeSort(iSchemaClass);
    }

    // try indexed functions
    Iterator<OIdentifiable> fetchedFromFunction = tryIndexedFunctions(iSchemaClass);
    if (fetchedFromFunction != null) {
      fetchFromTarget(fetchedFromFunction);
      return true;
    }

    // the main condition is a set of sub-conditions separated by OR operators
    final List<List<OIndexSearchResult>> conditionHierarchy =
        filterAnalyzer.analyzeMainCondition(
            compiledFilter.getRootCondition(), iSchemaClass, context);
    if (conditionHierarchy == null) return false;

    List<Stream<ORawPair<Object, ORID>>> streams = new ArrayList<>();

    boolean indexIsUsedInOrderBy = false;
    List<IndexUsageLog> indexUseAttempts = new ArrayList<IndexUsageLog>();
    try {

      OIndexSearchResult lastSearchResult = null;
      for (List<OIndexSearchResult> indexSearchResults : conditionHierarchy) {
        // go through all variants to choose which one can be used for index search.
        boolean indexUsed = false;
        for (final OIndexSearchResult searchResult : indexSearchResults) {
          lastSearchResult = searchResult;
          final List<OIndex> involvedIndexes =
              filterAnalyzer.getInvolvedIndexes(iSchemaClass, searchResult);

          Collections.sort(involvedIndexes, new IndexComparator());

          // go through all possible index for given set of fields.
          for (final OIndex index : involvedIndexes) {
            final OIndexDefinition indexDefinition = index.getDefinition();

            if (searchResult.containsNullValues && indexDefinition.isNullValuesIgnored()) {
              continue;
            }

            final OQueryOperator operator = searchResult.lastOperator;

            // we need to test that last field in query subset and field in index that has the same
            // position
            // are equals.
            if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
              final String lastFiled =
                  searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
              final String relatedIndexField =
                  indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
              if (!lastFiled.equals(relatedIndexField)) {
                continue;
              }
            }

            final int searchResultFieldsCount = searchResult.fields().size();
            final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
            // We get only subset contained in processed sub query.
            for (final String fieldName :
                indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
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

            Stream<ORawPair<Object, ORID>> stream;
            indexIsUsedInOrderBy =
                orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)
                    && !(index.getInternal() instanceof OChainedIndexProxy);
            try {
              boolean ascSortOrder =
                  !indexIsUsedInOrderBy || orderedFields.get(0).getValue().equals(KEYWORD_ASC);

              if (indexIsUsedInOrderBy) {
                fullySortedByIndex =
                    expandTarget == null
                        && indexDefinition.getFields().size() >= orderedFields.size()
                        && conditionHierarchy.size() == 1;
              }

              context.setVariable("$limit", limit);

              stream = operator.executeIndexQuery(context, index, keyParams, ascSortOrder);
              if (stream != null) {
                metricRecorder.recordInvolvedIndexesMetric(index);
              }

            } catch (OIndexEngineException e) {
              throw e;
            } catch (Exception e) {
              OLogManager.instance()
                  .error(
                      this,
                      "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                      e,
                      index.getName(),
                      request != null && request.getText() != null ? request.getText() : "");

              fullySortedByIndex = false;
              streams.clear();
              return false;
            }

            if (stream == null) {
              continue;
            }

            streams.add(stream);
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

      if (streams.size() == 0 || lastSearchResult == null) {
        return false;
      }

      if (streams.size() == 1 && canOptimize(conditionHierarchy)) {
        filterOptimizer.optimize(compiledFilter, lastSearchResult);
      }

      uniqueResult = new ConcurrentHashMap<ORID, ORID>();

      if (streams.size() == 1
          && (compiledFilter == null || compiledFilter.getRootCondition() == null)
          && groupByFields == null
          && projections != null
          && projections.size() == 1) {
        // OPTIMIZATION: ONE INDEX USED WITH JUST ONE CONDITION: REMOVE THE FILTER
        final Entry<String, Object> entry = projections.entrySet().iterator().next();

        if (entry.getValue() instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
          if (rf.function instanceof OSQLFunctionCount
              && rf.configuredParameters.length == 1
              && "*".equals(rf.configuredParameters[0])) {

            final boolean restrictedClasses = isUsingRestrictedClasses();

            if (!restrictedClasses) {
              final Iterator cursor = streams.get(0).iterator();
              long count = 0;
              if (cursor instanceof OSizeable) count = ((OSizeable) cursor).size();
              else {
                while (cursor.hasNext()) {
                  cursor.next();
                  count++;
                }
              }

              final OProfiler profiler = Orient.instance().getProfiler();
              if (profiler.isRecording()) {
                profiler.updateCounter(
                    profiler.getDatabaseMetric(database.getName(), "query.indexUsed"),
                    "Used index in query",
                    +1);
              }
              if (tempResult == null) tempResult = new ArrayList<OIdentifiable>();
              ((Collection<OIdentifiable>) tempResult)
                  .add(new ODocument().field(entry.getKey(), count));
              return true;
            }
          }
        }
      }

      for (Stream<ORawPair<Object, ORID>> stream : streams) {
        if (!fetchValuesFromIndexStream(stream)) {
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
        revertProfiler(
            context,
            wastedIndexUsage.index,
            wastedIndexUsage.keyParams,
            wastedIndexUsage.indexDefinition);
      }
    }
  }

  private Iterator<OIdentifiable> tryIndexedFunctions(OClass iSchemaClass) {
    // TODO profiler
    if (this.preParsedStatement == null) {
      return null;
    }
    OWhereClause where = ((OSelectStatement) this.preParsedStatement).getWhereClause();
    if (where == null) {
      return null;
    }
    List<OBinaryCondition> conditions =
        where.getIndexedFunctionConditions(iSchemaClass, getDatabase());

    long lastEstimation = Long.MAX_VALUE;
    OBinaryCondition bestCondition = null;
    if (conditions == null) {
      return null;
    }
    for (OBinaryCondition condition : conditions) {
      long estimation =
          condition.estimateIndexed(
              ((OSelectStatement) this.preParsedStatement).getTarget(), getContext());
      if (estimation > -1 && estimation < lastEstimation) {
        lastEstimation = estimation;
        bestCondition = condition;
      }
    }

    if (bestCondition == null) {
      return null;
    }
    Iterable<OIdentifiable> result =
        bestCondition.executeIndexedFunction(
            ((OSelectStatement) this.preParsedStatement).getTarget(), getContext());
    if (result == null) {
      return null;
    }
    return result.iterator();
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
   * @param iSchemaClass where search for indexes for optimization.
   * @return true if execution was optimized
   */
  private boolean optimizeSort(OClass iSchemaClass) {
    Stream<ORawPair<Object, ORID>> stream = getOptimizedSortStream(iSchemaClass);
    if (stream != null) {
      fetchValuesFromIndexStream(stream);
      return true;
    }
    return false;
  }

  private Stream<ORawPair<Object, ORID>> getOptimizedSortStream(OClass iSchemaClass) {
    final List<String> fieldNames = new ArrayList<String>();

    for (OPair<String, String> pair : orderedFields) {
      fieldNames.add(pair.getKey());
    }

    final Set<OIndex> indexes = iSchemaClass.getInvolvedIndexes(fieldNames);

    for (OIndex index : indexes) {
      if (orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)) {

        final boolean ascSortOrder = orderedFields.get(0).getValue().equals(KEYWORD_ASC);

        final List<Stream<ORawPair<Object, ORID>>> streams = new ArrayList<>();

        Stream<ORawPair<Object, ORID>> stream = null;

        if (ascSortOrder) {
          stream = index.getInternal().stream();
        } else {
          stream = index.getInternal().descStream();
        }

        if (stream != null) streams.add(stream);

        if (index.getMetadata() != null && !index.getDefinition().isNullValuesIgnored()) {
          @SuppressWarnings("resource")
          final Stream<ORID> nullRids = index.getInternal().getRids(null);
          streams.add(nullRids.map((rid) -> new ORawPair<>(null, rid)));
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

        if (streams.isEmpty()) {
          return Stream.empty();
        }

        if (streams.size() == 1) {
          return streams.get(0);
        }

        Stream<ORawPair<Object, ORID>> resultStream = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
          resultStream = Stream.concat(resultStream, streams.get(i));
        }

        return resultStream;
      }
    }

    metricRecorder.recordOrderByOptimizationMetric(false, this.fullySortedByIndex);
    return null;
  }

  private boolean fetchValuesFromIndexStream(final Stream<ORawPair<Object, ORID>> stream) {
    return fetchFromTarget(stream.map((pair) -> pair.second).iterator());
  }

  private void fetchEntriesFromIndexStream(final Stream<ORawPair<Object, ORID>> stream) {
    final Iterator<ORawPair<Object, ORID>> iterator = stream.iterator();

    while (iterator.hasNext()) {
      final ORawPair<Object, ORID> entryRecord = iterator.next();
      final ODocument doc = new ODocument().setOrdered(true);
      doc.field("key", entryRecord.first);
      doc.field("rid", entryRecord.second);
      ORecordInternal.unsetDirty(doc);

      applyGroupBy(doc, context);

      if (!handleResult(doc, context)) {
        // LIMIT REACHED
        break;
      }
    }
  }

  private boolean isRidOnlySort() {
    if (parsedTarget.getTargetClasses() != null
        && this.orderedFields.size() == 1
        && this.orderedFields.get(0).getKey().toLowerCase(Locale.ENGLISH).equals("@rid")) {
      if (this.target != null && target instanceof ORecordIteratorClass) {
        return true;
      }
    }
    return false;
  }

  private void applyOrderBy(boolean clearOrderedFields) {
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
      if (clearOrderedFields) {
        orderedFields.clear();
      }
    } finally {
      metricRecorder.orderByElapsed(startOrderBy);
    }
  }

  private Iterable<OIdentifiable> applySort(
      List<OIdentifiable> iCollection,
      List<OPair<String, String>> iOrderFields,
      OCommandContext iContext) {

    ODocumentHelper.sort(iCollection, iOrderFields, iContext);
    return iCollection;
  }

  /** Extract the content of collections and/or links and put it as result */
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
            } else if (r instanceof Iterator || OMultiValue.isMultiValue(r)) {
              for (Object o : OMultiValue.getMultiValueIterable(r)) {
                ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) o);
              }
            }
          }
        } else if (expandTarget instanceof OSQLFunctionRuntime
            && !hasFieldItemParams((OSQLFunctionRuntime) expandTarget)) {
          if (((OSQLFunctionRuntime) expandTarget).aggregateResults()) {
            throw new OCommandExecutionException(
                "Unsupported operation: aggregate function in expand(" + expandTarget + ")");
          } else {
            Object r = ((OSQLFunctionRuntime) expandTarget).execute(null, null, null, context);
            if (r instanceof OIdentifiable) {
              ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) r);
            } else if (r instanceof Iterator || OMultiValue.isMultiValue(r)) {
              for (Object o : OMultiValue.getMultiValueIterable(r)) {
                ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) o);
              }
            }
          }
        }
      } else {
        if (tempResult == null) {
          tempResult = new ArrayList<OIdentifiable>();
        }
        final OMultiCollectionIterator<OIdentifiable> finalResult =
            new OMultiCollectionIterator<OIdentifiable>();

        if (orderedFields == null || orderedFields.size() == 0) {
          // expand is applied before sorting, so limiting the result set here would give wrong
          // results
          int iteratorLimit = 0;
          if (limit < 0) {
            iteratorLimit = -1;
          } else {
            iteratorLimit += limit;
          }
          finalResult.setLimit(iteratorLimit);
          finalResult.setSkip(skip);
        }

        for (OIdentifiable id : tempResult) {
          Object fieldValue;
          if (expandTarget instanceof OSQLFilterItem) {
            fieldValue = ((OSQLFilterItem) expandTarget).getValue(id.getRecord(), null, context);
          } else if (expandTarget instanceof OSQLFunctionRuntime) {
            fieldValue = ((OSQLFunctionRuntime) expandTarget).getResult();
          } else {
            fieldValue = expandTarget.toString();
          }

          if (fieldValue != null) {
            if (fieldValue instanceof Iterable && !(fieldValue instanceof OIdentifiable)) {
              fieldValue = ((Iterable) fieldValue).iterator();
            }
            if (fieldValue instanceof ODocument) {
              ArrayList<ODocument> partial = new ArrayList<ODocument>();
              partial.add((ODocument) fieldValue);
              finalResult.add(partial);
            } else if (fieldValue instanceof Collection<?>
                || fieldValue.getClass().isArray()
                || fieldValue instanceof Iterator<?>
                || fieldValue instanceof OIdentifiable
                || fieldValue instanceof ORidBag) {
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

  private boolean hasFieldItemParams(OSQLFunctionRuntime expandTarget) {
    Object[] params = expandTarget.getConfiguredParameters();
    if (params == null) {
      return false;
    }
    for (Object o : params) {
      if (o instanceof OSQLFilterItemField) {
        return true;
      }
    }
    return false;
  }

  private void searchInIndex() {
    OIndexAbstract.manualIndexesWarning();

    final ODatabaseDocumentInternal database = getDatabase();
    final OIndex index =
        (OIndex)
            database
                .getMetadata()
                .getIndexManagerInternal()
                .getIndex(database, parsedTarget.getTargetIndex());

    if (index == null) {
      throw new OCommandExecutionException(
          "Target index '" + parsedTarget.getTargetIndex() + "' not found");
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

        try (Stream<ORawPair<Object, ORID>> stream =
            index
                .getInternal()
                .streamEntriesBetween(
                    getIndexKey(index.getDefinition(), values[0], context),
                    true,
                    getIndexKey(index.getDefinition(), values[2], context),
                    true,
                    ascOrder)) {
          fetchEntriesFromIndexStream(stream);
        }
      } else if (indexOperator instanceof OQueryOperatorMajor) {
        final Object value = compiledFilter.getRootCondition().getRight();

        try (Stream<ORawPair<Object, ORID>> stream =
            index
                .getInternal()
                .streamEntriesMajor(
                    getIndexKey(index.getDefinition(), value, context), false, ascOrder)) {
          fetchEntriesFromIndexStream(stream);
        }
      } else if (indexOperator instanceof OQueryOperatorMajorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        try (Stream<ORawPair<Object, ORID>> stream =
            index
                .getInternal()
                .streamEntriesMajor(
                    getIndexKey(index.getDefinition(), value, context), true, ascOrder)) {
          fetchEntriesFromIndexStream(stream);
        }

      } else if (indexOperator instanceof OQueryOperatorMinor) {
        final Object value = compiledFilter.getRootCondition().getRight();

        try (Stream<ORawPair<Object, ORID>> stream =
            index
                .getInternal()
                .streamEntriesMinor(
                    getIndexKey(index.getDefinition(), value, context), false, ascOrder)) {
          fetchEntriesFromIndexStream(stream);
        }
      } else if (indexOperator instanceof OQueryOperatorMinorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();

        try (Stream<ORawPair<Object, ORID>> stream =
            index
                .getInternal()
                .streamEntriesMinor(
                    getIndexKey(index.getDefinition(), value, context), true, ascOrder)) {
          fetchEntriesFromIndexStream(stream);
        }
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

        try (Stream<ORawPair<Object, ORID>> stream =
            index.getInternal().streamEntries(values, true)) {
          fetchEntriesFromIndexStream(stream);
        }
      } else {
        final Object right = compiledFilter.getRootCondition().getRight();
        Object keyValue = getIndexKey(index.getDefinition(), right, context);
        if (keyValue == null) {
          return;
        }

        final Stream<ORID> res;
        if (index.getDefinition().getParamCount() == 1) {
          // CONVERT BEFORE SEARCH IF NEEDED
          final OType type = index.getDefinition().getTypes()[0];
          keyValue = OType.convert(keyValue, type.getDefaultJavaType());

          //noinspection resource
          res = index.getInternal().getRids(keyValue);
        } else {
          final Object secondKey = getIndexKey(index.getDefinition(), right, context);
          if (keyValue instanceof OCompositeKey
              && secondKey instanceof OCompositeKey
              && ((OCompositeKey) keyValue).getKeys().size()
                  == index.getDefinition().getParamCount()
              && ((OCompositeKey) secondKey).getKeys().size()
                  == index.getDefinition().getParamCount()) {
            //noinspection resource
            res = index.getInternal().getRids(keyValue);
          } else {
            try (Stream<ORawPair<Object, ORID>> stream =
                index.getInternal().streamEntriesBetween(keyValue, true, secondKey, true, true)) {
              fetchEntriesFromIndexStream(stream);
            }
            return;
          }
        }

        final Object resultKey = keyValue;
        BreakingForEach.forEach(
            res,
            (rid, breaker) -> {
              final ODocument record = createIndexEntryAsDocument(resultKey, rid);
              applyGroupBy(record, context);
              if (!handleResult(record, context)) {
                // LIMIT REACHED
                breaker.stop();
              }
            });
      }

    } else {
      if (isIndexSizeQuery()) {
        getProjectionGroup(null, context)
            .applyValue(projections.keySet().iterator().next(), index.getInternal().size());
        return;
      }

      if (isIndexKeySizeQuery()) {
        getProjectionGroup(null, context)
            .applyValue(projections.keySet().iterator().next(), index.getInternal().size());
        return;
      }

      final OIndexInternal indexInternal = index.getInternal();
      if (indexInternal instanceof OSharedResource) {
        ((OSharedResource) indexInternal).acquireExclusiveLock();
      }

      try {

        // ADD ALL THE ITEMS AS RESULT
        if (ascOrder) {
          try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
            fetchEntriesFromIndexStream(stream);
          }
          fetchNullKeyEntries(index);
        } else {

          try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().descStream()) {
            fetchNullKeyEntries(index);
            fetchEntriesFromIndexStream(stream);
          }
        }
      } finally {
        if (indexInternal instanceof OSharedResource) {
          ((OSharedResource) indexInternal).releaseExclusiveLock();
        }
      }
    }
  }

  private void fetchNullKeyEntries(OIndex index) {
    if (index.getDefinition().isNullValuesIgnored()) {
      return;
    }

    final Stream<ORID> rids = index.getInternal().getRids(null);
    BreakingForEach.forEach(
        rids,
        (rid, breaker) -> {
          final ODocument doc = new ODocument().setOrdered(true);
          doc.field("key", (Object) null);
          doc.field("rid", rid);
          ORecordInternal.unsetDirty(doc);

          applyGroupBy(doc, context);

          if (!handleResult(doc, context)) {
            // LIMIT REACHED
            breaker.stop();
          }
        });
  }

  private boolean isIndexSizeQuery() {
    if (!(aggregate && projections.entrySet().size() == 1)) {
      return false;
    }

    final Object projection = projections.values().iterator().next();
    if (!(projection instanceof OSQLFunctionRuntime)) {
      return false;
    }

    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
    return f.getRoot().equals(OSQLFunctionCount.NAME)
        && ((f.configuredParameters == null || f.configuredParameters.length == 0)
            || (f.configuredParameters.length == 1 && f.configuredParameters[0].equals("*")));
  }

  private boolean isIndexKeySizeQuery() {
    if (!(aggregate && projections.entrySet().size() == 1)) {
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

    if (!(f.configuredParameters != null
        && f.configuredParameters.length == 1
        && f.configuredParameters[0] instanceof OSQLFunctionRuntime)) {
      return false;
    }

    final OSQLFunctionRuntime fConfigured = (OSQLFunctionRuntime) f.configuredParameters[0];
    if (!fConfigured.getRoot().equals(OSQLFunctionDistinct.NAME)) {
      return false;
    }

    if (!(fConfigured.configuredParameters != null
        && fConfigured.configuredParameters.length == 1
        && fConfigured.configuredParameters[0] instanceof OSQLFilterItemField)) {
      return false;
    }

    final OSQLFilterItemField field = (OSQLFilterItemField) fConfigured.configuredParameters[0];
    return field.getRoot().equals("key");
  }

  private void handleNoTarget() {
    if (parsedTarget == null && expandTarget == null)
      // ONLY LET, APPLY TO THEM
      addResult(ORuntimeResult.createProjectionDocument(getTemporaryRIDCounter(context)), context);
  }

  private void handleGroupBy(final OCommandContext iContext) {
    if (aggregate && tempResult == null) {

      final long startGroupBy = System.currentTimeMillis();
      try {

        tempResult = new ArrayList<OIdentifiable>();

        for (Entry<Object, ORuntimeResult> g : groupedResult.entrySet()) {
          if (g.getKey() != null || (groupedResult.size() == 1 && groupByFields == null)) {
            final ODocument doc = g.getValue().getResult();
            if (doc != null) {
              ((List<OIdentifiable>) tempResult).add(doc);
            }
          }
        }

      } finally {
        iContext.setVariable("groupByElapsed", (System.currentTimeMillis() - startGroupBy));
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
