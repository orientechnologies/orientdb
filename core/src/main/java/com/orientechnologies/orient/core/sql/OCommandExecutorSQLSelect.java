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

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.operator.*;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

import java.util.*;
import java.util.Map.Entry;

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
  public static final String          KEYWORD_FETCHPLAN    = "FETCHPLAN";
  private static final String         KEYWORD_AS           = " AS ";
  private Map<String, String>         projectionDefinition = null;
  private Map<String, Object>         projections          = null;                                  // THIS HAS BEEN KEPT FOR
                                                                                                     // COMPATIBILITY; BUT
  // IT'S
  // USED THE
  // PROJECTIONS IN GROUPED-RESULTS
  private List<OPair<String, String>> orderedFields        = new ArrayList<OPair<String, String>>();
  private List<String>                groupByFields;
  private Map<Object, ORuntimeResult> groupedResult;
  private Object                      expandTarget;
  private int                         fetchLimit           = -1;
  private OIdentifiable               lastRecord;
  private Iterator<OIdentifiable>     subIterator;
  private String                      fetchPlan;

  private boolean                     fullySortedByIndex   = false;

  private final class IndexComparator implements Comparator<OIndex<?>> {
    public int compare(final OIndex<?> indexOne, final OIndex<?> indexTwo) {
      final OIndexDefinition definitionOne = indexOne.getDefinition();
      final OIndexDefinition definitionTwo = indexTwo.getDefinition();

      final int firstParamCount = definitionOne.getParamCount();
      final int secondParamCount = definitionTwo.getParamCount();

      final int result = firstParamCount - secondParamCount;

      if (result == 0 && !orderedFields.isEmpty()) {
        if (!(indexOne instanceof OChainedIndexProxy) && canBeUsedByOrderBy(indexOne))
          return 1;

        if (!(indexTwo instanceof OChainedIndexProxy) && canBeUsedByOrderBy(indexTwo))
          return -1;
      }

      return result;
    }
  }

  private final class IndexResultListener implements OQueryOperator.IndexResultListener {
    private final List<OIdentifiable> result = new ArrayList<OIdentifiable>();
    private final int                 fetchLimit;
    private final int                 skip;

    private IndexResultListener(int fetchLimit, int skip) {
      this.fetchLimit = fetchLimit;
      this.skip = skip;
    }

    @Override
    public Object getResult() {
      return result;
    }

    @Override
    public boolean addResult(OIdentifiable value) {
      final ORecord record = value.getRecord();

      if (record instanceof ORecordSchemaAware<?>) {
        final ORecordSchemaAware<?> recordSchemaAware = (ORecordSchemaAware<?>) record;
        final Map<OClass, String> targetClasses = parsedTarget.getTargetClasses();
        if ((targetClasses != null) && (!targetClasses.isEmpty())) {
          for (OClass targetClass : targetClasses.keySet()) {
            if (!targetClass.isSuperClassOf(recordSchemaAware.getSchemaClass()))
              return true;
          }
        }
      }

      if (compiledFilter == null || Boolean.TRUE.equals(compiledFilter.evaluate(value.getRecord(), null, context)))
        result.add(value);

      return fetchLimit < 0 || result.size() < fetchLimit + skip;
    }
  }

  protected static OSQLFilterCondition getConditionForRidPosRange(long fromId, long toId) {

    final OSQLFilterCondition fromCondition = new OSQLFilterCondition(new OSQLFilterItemField(null,
        ODocumentHelper.ATTRIBUTE_RID_POS), new OQueryOperatorMajor(), fromId);
    final OSQLFilterCondition toCondition = new OSQLFilterCondition(
        new OSQLFilterItemField(null, ODocumentHelper.ATTRIBUTE_RID_POS), new OQueryOperatorMinorEquals(), toId);

    return new OSQLFilterCondition(fromCondition, new OQueryOperatorAnd(), toCondition);
  }

  private static List<OIndex<?>> getInvolvedIndexes(OClass iSchemaClass, OIndexSearchResult searchResultFields) {
    final Set<OIndex<?>> involvedIndexes = iSchemaClass.getInvolvedIndexes(searchResultFields.fields());

    final List<OIndex<?>> result = new ArrayList<OIndex<?>>(involvedIndexes.size());
    for (OIndex<?> involvedIndex : involvedIndexes) {
      if (searchResultFields.lastField.isLong()) {
        result.addAll(OChainedIndexProxy.createdProxy(involvedIndex, searchResultFields.lastField, getDatabase()));
      } else {
        result.add(involvedIndex);
      }
    }

    return result;
  }

  private static OIndexSearchResult analyzeQueryBranch(final OClass iSchemaClass, OSQLFilterCondition iCondition,
      final List<OIndexSearchResult> iIndexSearchResults) {
    if (iCondition == null)
      return null;

    OQueryOperator operator = iCondition.getOperator();

    while (operator == null) {
      if (iCondition.getRight() == null && iCondition.getLeft() instanceof OSQLFilterCondition) {
        iCondition = (OSQLFilterCondition) iCondition.getLeft();
        operator = iCondition.getOperator();
      } else {
        return null;
      }
    }

    final OIndexReuseType indexReuseType = operator.getIndexReuseType(iCondition.getLeft(), iCondition.getRight());
    if (indexReuseType.equals(OIndexReuseType.INDEX_INTERSECTION)) {
      final OIndexSearchResult leftResult = analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getLeft(),
          iIndexSearchResults);
      final OIndexSearchResult rightResult = analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getRight(),
          iIndexSearchResults);

      if (leftResult != null && rightResult != null) {
        if (leftResult.canBeMerged(rightResult)) {
          final OIndexSearchResult mergeResult = leftResult.merge(rightResult);
          if (iSchemaClass.areIndexed(mergeResult.fields()))
            iIndexSearchResults.add(mergeResult);
          return leftResult.merge(rightResult);
        }
      }

      return null;
    } else if (indexReuseType.equals(OIndexReuseType.INDEX_METHOD)) {
      OIndexSearchResult result = createIndexedProperty(iCondition, iCondition.getLeft());
      if (result == null)
        result = createIndexedProperty(iCondition, iCondition.getRight());

      if (result == null)
        return null;

      if (checkIndexExistence(iSchemaClass, result))
        iIndexSearchResults.add(result);

      return result;
    }

    return null;
  }

  /**
   * Add SQL filter field to the search candidate list.
   * 
   * @param iCondition
   *          Condition item
   * @param iItem
   *          Value to search
   * @return true if the property was indexed and found, otherwise false
   */
  private static OIndexSearchResult createIndexedProperty(final OSQLFilterCondition iCondition, final Object iItem) {
    if (iItem == null || !(iItem instanceof OSQLFilterItemField))
      return null;

    if (iCondition.getLeft() instanceof OSQLFilterItemField && iCondition.getRight() instanceof OSQLFilterItemField)
      return null;

    final OSQLFilterItemField item = (OSQLFilterItemField) iItem;

    if (item.hasChainOperators() && !item.isFieldChain())
      return null;

    final Object origValue = iCondition.getLeft() == iItem ? iCondition.getRight() : iCondition.getLeft();

    if (iCondition.getOperator() instanceof OQueryOperatorBetween || iCondition.getOperator() instanceof OQueryOperatorIn) {
      return new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), origValue);
    }

    final Object value = OSQLHelper.getValue(origValue);

    if (value == null)
      return null;

    return new OIndexSearchResult(iCondition.getOperator(), item.getFieldChain(), value);
  }

  private static Object getIndexKey(final OIndexDefinition indexDefinition, Object value, OCommandContext context) {
    if (indexDefinition instanceof OCompositeIndexDefinition) {
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
      return OSQLHelper.getValue(value);
    }
  }

  private static ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
    final ODocument doc = new ODocument().setOrdered(true);
    doc.field("key", iKey);
    doc.field("rid", iValue);
    doc.unsetDirty();
    return doc;
  }

  private static boolean checkIndexExistence(final OClass iSchemaClass, final OIndexSearchResult result) {
    if (!iSchemaClass.areIndexed(result.fields()))
      return false;

    if (result.lastField.isLong()) {
      final int fieldCount = result.lastField.getItemCount();
      OClass cls = iSchemaClass.getProperty(result.lastField.getItemName(0)).getLinkedClass();

      for (int i = 1; i < fieldCount; i++) {
        if (cls == null || !cls.areIndexed(result.lastField.getItemName(i))) {
          return false;
        }

        cls = cls.getProperty(result.lastField.getItemName(i)).getLinkedClass();
      }
    }
    return true;
  }

  /**
   * Compile the filter conditions only the first time.
   */
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    super.parse(iRequest);

    if (context == null)
      context = new OBasicCommandContext();

    final int pos = parseProjections();
    if (pos == -1)
      return this;

    final int endPosition = parserText.length();

    parserNextWord(true);
    if (parserGetLastWord().equalsIgnoreCase(KEYWORD_FROM)) {
      // FROM
      parsedTarget = OSQLEngine.getInstance().parseTarget(parserText.substring(parserGetCurrentPosition(), endPosition),
          getContext(), KEYWORD_WHERE);
      parserSetCurrentPosition(parsedTarget.parserIsEnded() ? endPosition : parsedTarget.parserGetCurrentPosition()
          + parserGetCurrentPosition());
    } else
      parserGoBack();

    if (!parserIsEnded()) {
      parserSkipWhiteSpaces();

      while (!parserIsEnded()) {
        parserNextWord(true);

        if (!parserIsEnded()) {

          final String w = parserGetLastWord();

          if (w.equals(KEYWORD_WHERE)) {
            compiledFilter = OSQLEngine.getInstance().parseCondition(parserText.substring(parserGetCurrentPosition(), endPosition),
                getContext(), KEYWORD_WHERE);
            optimize();
            parserSetCurrentPosition(compiledFilter.parserIsEnded() ? endPosition : compiledFilter.parserGetCurrentPosition()
                + parserGetCurrentPosition());
          } else if (w.equals(KEYWORD_LET))
            parseLet();
          else if (w.equals(KEYWORD_GROUP))
            parseGroupBy(w);
          else if (w.equals(KEYWORD_ORDER))
            parseOrderBy(w);
          else if (w.equals(KEYWORD_LIMIT))
            parseLimit(w);
          else if (w.equals(KEYWORD_SKIP) || w.equals(KEYWORD_OFFSET))
            parseSkip(w);
          else if (w.equals(KEYWORD_FETCHPLAN))
            parseFetchplan(w);
          else if (w.equals(KEYWORD_TIMEOUT))
            parseTimeout(w);
          else
            throwParsingException("Invalid keyword '" + w + "'");
        }
      }
    }
    if (limit == 0 || limit < -1) {
      throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
    }

    return this;
  }

  /**
   * Determine clusters that are used in select operation
   * 
   * @return set of involved clusters
   */
  public Set<Integer> getInvolvedClusters() {

    final Set<Integer> clusters = new HashSet<Integer>();
    if (parsedTarget.getTargetRecords() != null) {
      for (OIdentifiable identifiable : parsedTarget.getTargetRecords()) {
        clusters.add(identifiable.getIdentity().getClusterId());
      }
    }
    if (parsedTarget.getTargetClasses() != null) {
      final OStorage storage = getDatabase().getStorage();
      for (String clazz : parsedTarget.getTargetClasses().values()) {
        clusters.add(storage.getClusterIdByName(clazz));
      }
    }
    if (parsedTarget.getTargetClusters() != null) {
      final OStorage storage = getDatabase().getStorage();
      for (String clazz : parsedTarget.getTargetClusters().values()) {
        clusters.add(storage.getClusterIdByName(clazz));
      }
    }
    if (parsedTarget.getTargetIndex() != null) {
      // TODO indexes??
    }
    return clusters;
  }

  /**
   * Add condition so that query will be executed only on the given id range. That is used to verify that query will be executed on
   * the single node
   * 
   * @param fromId
   * @param toId
   * @return this
   */
  public OCommandExecutorSQLSelect boundToLocalNode(long fromId, long toId) {
    if (fromId == toId) {
      // single node in dht
      return this;
    }

    final OSQLFilterCondition nodeCondition;
    if (fromId < toId) {
      nodeCondition = getConditionForRidPosRange(fromId, toId);
    } else {
      nodeCondition = new OSQLFilterCondition(getConditionForRidPosRange(fromId, Long.MAX_VALUE), new OQueryOperatorOr(),
          getConditionForRidPosRange(-1L, toId));
    }

    if (compiledFilter == null) {
      compiledFilter = OSQLEngine.getInstance().parseCondition("", getContext(), KEYWORD_WHERE);
    }

    final OSQLFilterCondition rootCondition = compiledFilter.getRootCondition();
    if (rootCondition != null) {
      compiledFilter.setRootCondition(new OSQLFilterCondition(nodeCondition, new OQueryOperatorAnd(), rootCondition));
    } else {
      compiledFilter.setRootCondition(nodeCondition);
    }
    return this;
  }

  /**
   * @return {@code ture} if any of the sql functions perform aggregation, {@code false} otherwise
   */
  public boolean isAnyFunctionAggregates() {
    if (projections != null) {
      for (Entry<String, Object> p : projections.entrySet()) {
        if (p.getValue() instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) p.getValue()).aggregateResults())
          return true;
      }
    }
    return false;
  }

  public Iterator<OIdentifiable> iterator() {
    return iterator(null);
  }

  public Iterator<OIdentifiable> iterator(final Map<Object, Object> iArgs) {
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
    } else
      subIterator = (Iterator<OIdentifiable>) target;

    return subIterator;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    try {
      if (iArgs != null)
        // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
        for (Entry<Object, Object> arg : iArgs.entrySet())
          context.setVariable(arg.getKey().toString(), arg.getValue());

      if (timeoutMs > 0)
        getContext().beginExecution(timeoutMs, timeoutStrategy);

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
      if (request.getResultListener() != null)
        request.getResultListener().end();
    }
  }

  public Map<String, Object> getProjections() {
    return projections;
  }

  public List<OPair<String, String>> getOrderedFields() {
    return orderedFields;
  }

  @Override
  public String getSyntax() {
    return "SELECT [<Projections>] FROM <Target> [LET <Assignment>*] [WHERE <Condition>*] [ORDER BY <Fields>* [ASC|DESC]*] [LIMIT <MaxRecords>] TIMEOUT <TimeoutInMs>";
  }

  public String getFetchPlan() {
    return fetchPlan != null ? fetchPlan : request.getFetchPlan();
  }

  protected void executeSearch(final Map<Object, Object> iArgs) {
    assignTarget(iArgs);

    if (target == null) {
      if (let != null)
        // EXECUTE ONCE TO ASSIGN THE LET
        assignLetClauses(lastRecord != null ? lastRecord.getRecord() : null);

      // SEARCH WITHOUT USING TARGET (USUALLY WHEN LET/INDEXES ARE INVOLVED)
      return;
    }

    final long startFetching = System.currentTimeMillis();
    try {

      // BROWSE ALL THE RECORDS
      while (target.hasNext())
        if (!executeSearchRecord(target.next()))
          break;

    } finally {
      context.setVariable("fetchingFromTargetElapsed", (System.currentTimeMillis() - startFetching));
    }
  }

  @Override
  protected boolean assignTarget(Map<Object, Object> iArgs) {
    if (!super.assignTarget(iArgs)) {
      if (parsedTarget.getTargetIndex() != null)
        searchInIndex();
      else
        throw new OQueryParsingException("No source found in query: specify class, cluster(s), index or single record(s). Use "
            + getSyntax());
    }
    return true;
  }

  protected boolean executeSearchRecord(final OIdentifiable id) {
    if (Thread.interrupted())
      throw new OCommandExecutionException("The select execution has been interrupted");

    if (!context.checkTimeout())
      return false;

    final OStorage.LOCKING_STRATEGY lockingStrategy = context.getVariable("$locking") != null ? (OStorage.LOCKING_STRATEGY) context
        .getVariable("$locking") : OStorage.LOCKING_STRATEGY.DEFAULT;
    ORecordInternal<?> record = null;
    try {
      if (id instanceof ORecordInternal<?>) {
        record = (ORecordInternal<?>) id;

        // LOCK THE RECORD IF NEEDED
        if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK)
          ((OStorageEmbedded) getDatabase().getStorage()).acquireWriteLock(record.getIdentity());
        else if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK)
          ((OStorageEmbedded) getDatabase().getStorage()).acquireReadLock(record.getIdentity());

      } else
        record = getDatabase().load(id.getIdentity(), null, false, false, lockingStrategy);

      context.updateMetric("recordReads", +1);

      if (record == null || record.getRecordType() != ODocument.RECORD_TYPE)
        // SKIP IT
        return true;

      context.updateMetric("documentReads", +1);

      if (filter(record))
        if (!handleResult(record, true))
          // END OF EXECUTION
          return false;
    } finally {
      // lock must be released (no matter if filtered or not)
      if (record != null)
        if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK) {
          ((OStorageEmbedded) getDatabase().getStorage()).releaseWriteLock(record.getIdentity());
        } else if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK)
          ((OStorageEmbedded) getDatabase().getStorage()).releaseReadLock(record.getIdentity());
    }
    return true;
  }

  protected boolean handleResult(final OIdentifiable iRecord, final boolean iCloneIt) {
    lastRecord = null;

    if ((orderedFields.isEmpty() || fullySortedByIndex) && skip > 0) {
      skip--;
      return true;
    }

    if (iCloneIt)
      lastRecord = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();
    else
      lastRecord = iRecord;

    resultCount++;

    boolean result = addResult(lastRecord);
    if (!result)
      return false;

    if ((orderedFields.isEmpty() || fullySortedByIndex) && !isAnyFunctionAggregates() && fetchLimit > -1
        && resultCount >= fetchLimit)
      // BREAK THE EXECUTION
      return false;

    return true;
  }

  protected boolean addResult(OIdentifiable iRecord) {
    if (iRecord == null)
      return true;

    if (projections != null || groupByFields != null && !groupByFields.isEmpty()) {
      if (groupedResult == null) {
        // APPLY PROJECTIONS IN LINE
        iRecord = ORuntimeResult.getProjectionResult(resultCount, projections, context, iRecord);
        if (iRecord == null)
          return true;
      } else {
        // AGGREGATION/GROUP BY
        final ODocument doc = (ODocument) iRecord.getRecord();
        Object fieldValue = null;
        if (groupByFields != null && !groupByFields.isEmpty()) {
          if (groupByFields.size() > 1) {
            // MULTI-FIELD FROUP BY
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
                fieldValue = doc.field(field);
            }
          }
        }

        getProjectionGroup(fieldValue).applyRecord(iRecord);
        return true;
      }
    }

    boolean result = true;
    if ((fullySortedByIndex || orderedFields.isEmpty()) && expandTarget == null) {
      // SEND THE RESULT INLINE
      if (request.getResultListener() != null)
        result = request.getResultListener().result(iRecord);

    } else {

      // COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
      if (tempResult == null)
        tempResult = new ArrayList<OIdentifiable>();
      ((Collection<OIdentifiable>) tempResult).add(iRecord);
    }

    return result;
  }

  protected ORuntimeResult getProjectionGroup(final Object fieldValue) {
    ORuntimeResult group = null;

<<<<<<< HEAD
    Object key = null;
    if (groupedResult == null)
      groupedResult = new LinkedHashMap<Object, ORuntimeResult>();

    if (fieldValue != null) {
      if (fieldValue.getClass().isArray()) {
        // LOOK IT BY HASH (FASTER THAN COMPARE EACH SINGLE VALUE)
        final Object[] array = (Object[]) fieldValue;

        final StringBuilder keyArray = new StringBuilder();
<<<<<<< Updated upstream
        for (Object o : array) {
          if (keyArray.length() > 0)
            keyArray.append(",");
          if (o != null)
            keyArray.append(o instanceof OIdentifiable ? ((OIdentifiable) o).getIdentity().toString() : o.toString());
=======
        for (Object o : array){
          if( keyArray.length() > 0 )
            keyArray.append(",");
          if (o != null)
            keyArray.append(o instanceof OIdentifiable ? ((OIdentifiable)o).getIdentity().toString() : o.toString());
>>>>>>> Stashed changes
          else
            keyArray.append("null");
        }

        key = keyArray.toString();
      } else
        // LOKUP FOR THE FIELD
<<<<<<< Updated upstream
        key = fieldValue;
=======
        key = fieldValue.toString();

      group = groupedResult.get(key);
>>>>>>> Stashed changes
    }

    group = groupedResult.get(key);
    if (group == null) {
      group = new ORuntimeResult(fieldValue, createProjectionFromDefinition(), resultCount, context);
<<<<<<< Updated upstream
      groupedResult.put(key, group);
=======
      if (fieldValue != null && fieldValue.getClass().isArray())
        groupedResult.put(key, group);
      else
        groupedResult.put(fieldValue, group);
>>>>>>> Stashed changes
    }
    return group;
  }
=======
    final long projectionElapsed = (Long) context.getVariable("projectionElapsed", 0l);
    final long begin = System.currentTimeMillis();
    try {

      Object key = null;
      if (groupedResult == null)
        groupedResult = new LinkedHashMap<Object, ORuntimeResult>();

      if (fieldValue != null) {
        if (fieldValue.getClass().isArray()) {
          // LOOK IT BY HASH (FASTER THAN COMPARE EACH SINGLE VALUE)
          final Object[] array = (Object[]) fieldValue;

          final StringBuilder keyArray = new StringBuilder();
          for (Object o : array) {
            if (keyArray.length() > 0)
              keyArray.append(",");
            if (o != null)
              keyArray.append(o instanceof OIdentifiable ? ((OIdentifiable) o).getIdentity().toString() : o.toString());
            else
              keyArray.append("null");
          }

          key = keyArray.toString();
        } else
          // LOOKUP FOR THE FIELD
          key = fieldValue;
      }
>>>>>>> develop

      group = groupedResult.get(key);
      if (group == null) {
        group = new ORuntimeResult(fieldValue, createProjectionFromDefinition(), resultCount, context);
        groupedResult.put(key, group);
      }
      return group;
    } finally {
      context.setVariable("projectionElapsed", projectionElapsed + (System.currentTimeMillis() - begin));
    }
  }

  protected void parseGroupBy(final String w) {
    parserRequiredKeyword(KEYWORD_BY);

    groupByFields = new ArrayList<String>();
    while (!parserIsEnded() && (groupByFields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");
      groupByFields.add(fieldName);
      parserSkipWhiteSpaces();
    }

    if (groupByFields.size() == 0)
      throwParsingException("Group by field set was missed. Example: GROUP BY name, salary");

    // AGGREGATE IT
    getProjectionGroup(null);
  }

  protected void parseOrderBy(final String w) {
    parserRequiredKeyword(KEYWORD_BY);

    String fieldOrdering = null;

    orderedFields = new ArrayList<OPair<String, String>>();
    while (!parserIsEnded() && (orderedFields.size() == 0 || parserGetLastSeparator() == ',' || parserGetCurrentChar() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");

      parserOptionalWord(true);

      final String word = parserGetLastWord();

      if (word.length() == 0)
        // END CLAUSE: SET AS ASC BY DEFAULT
        fieldOrdering = KEYWORD_ASC;
      else if (word.equals(KEYWORD_LIMIT) || word.equals(KEYWORD_SKIP) || word.equals(KEYWORD_OFFSET)) {
        // NEXT CLAUSE: SET AS ASC BY DEFAULT
        fieldOrdering = KEYWORD_ASC;
        parserGoBack();
      } else {
        if (word.equals(KEYWORD_ASC))
          fieldOrdering = KEYWORD_ASC;
        else if (word.equals(KEYWORD_DESC))
          fieldOrdering = KEYWORD_DESC;
        else
          throwParsingException("Ordering mode '" + word + "' not supported. Valid is 'ASC', 'DESC' or nothing ('ASC' by default)");
      }

      orderedFields.add(new OPair<String, String>(fieldName, fieldOrdering));
      parserSkipWhiteSpaces();
    }

    if (orderedFields.size() == 0)
      throwParsingException("Order by field set was missed. Example: ORDER BY name ASC, salary DESC");
  }

  @Override
  protected void searchInClasses() {
    final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();

    if (searchForIndexes(cls)) {
    } else
      super.searchInClasses();
  }

  protected int parseProjections() {
    if (!parserOptionalKeyword(KEYWORD_SELECT))
      return -1;

    int upperBound = OStringSerializerHelper.getLowerIndexOf(parserTextUpperCase, parserGetCurrentPosition(), KEYWORD_FROM_2FIND,
        KEYWORD_LET_2FIND);
    if (upperBound == -1)
      // UP TO THE END
      upperBound = parserText.length();

    int lastRealPositionProjection = -1;

    final String projectionString = parserText.substring(parserGetCurrentPosition(), upperBound).trim();
    if (projectionString.length() > 0) {
      // EXTRACT PROJECTIONS
      projections = new LinkedHashMap<String, Object>();
      projectionDefinition = new LinkedHashMap<String, String>();

      final List<String> items = OStringSerializerHelper.smartSplit(projectionString, ',');

      String fieldName;
      int beginPos;
      int endPos;
      for (String projection : items) {
        projection = projection.trim();

        if (projectionDefinition == null)
          throw new OCommandSQLParsingException("Projection not allowed with FLATTEN() and EXPAND() operators");

        final List<String> words = OStringSerializerHelper.smartSplit(projection, ' ');
        if (words.size() > 1)
          lastRealPositionProjection = words.get(0).length();

        fieldName = null;
        endPos = projection.toUpperCase(Locale.ENGLISH).indexOf(KEYWORD_AS);
        if (endPos > -1) {
          // EXTRACT ALIAS
          fieldName = projection.substring(endPos + KEYWORD_AS.length()).trim();
          lastRealPositionProjection = endPos + KEYWORD_AS.length() + fieldName.length() + 1;
          projection = projection.substring(0, endPos).trim();

          if (projectionDefinition.containsKey(fieldName))
            throw new OCommandSQLParsingException("Field '" + fieldName
                + "' is duplicated in current SELECT, choose a different name");
        } else {
          // EXTRACT THE FIELD NAME WITHOUT FUNCTIONS AND/OR LINKS
          beginPos = projection.charAt(0) == '@' ? 1 : 0;

          endPos = extractProjectionNameSubstringEndPosition(projection);

          fieldName = endPos > -1 ? projection.substring(beginPos, endPos) : projection.substring(beginPos);

          fieldName = OStringSerializerHelper.getStringContent(fieldName);

          // FIND A UNIQUE NAME BY ADDING A COUNTER
          for (int fieldIndex = 2; projectionDefinition.containsKey(fieldName); ++fieldIndex)
            fieldName += fieldIndex;
        }

        String p = projection.toUpperCase(Locale.ENGLISH);
        if (p.startsWith("FLATTEN(") || p.startsWith("EXPAND(")) {
          if (p.startsWith("FLATTEN("))
            OLogManager.instance().debug(this, "FLATTEN() operator has been replaced by EXPAND()");
          List<String> pars = OStringSerializerHelper.getParameters(projection);
          if (pars.size() != 1) {
            throw new OCommandSQLParsingException(
                "EXPAND/FLATTEN operators expects the field name as parameter. Example EXPAND( out )");
          }
          expandTarget = OSQLHelper.parseValue(this, pars.get(0).trim(), context);

          // BY PASS THIS AS PROJECTION BUT TREAT IT AS SPECIAL
          projectionDefinition = null;
          projections = null;

          if (groupedResult == null && expandTarget instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) expandTarget).aggregateResults())
            getProjectionGroup(null);

          continue;
        }

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
      parserMoveCurrentPosition(lastRealPositionProjection + 1);
    else
      parserSetEndOfText();

    return parserGetCurrentPosition();
  }

  protected Map<String, Object> createProjectionFromDefinition() {
    if (projectionDefinition == null)
      return new LinkedHashMap<String, Object>();

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
    if (pos1 > -1 && pos2 == -1 && pos3 == -1)
      endPos = pos1;
    else if (pos2 > -1 && pos1 == -1 && pos3 == -1)
      endPos = pos2;
    else if (pos3 > -1 && pos1 == -1 && pos2 == -1)
      endPos = pos3;
    else if (pos1 > -1 && pos2 > -1 && pos3 == -1)
      endPos = Math.min(pos1, pos2);
    else if (pos2 > -1 && pos3 > -1 && pos1 == -1)
      endPos = Math.min(pos2, pos3);
    else if (pos1 > -1 && pos3 > -1 && pos2 == -1)
      endPos = Math.min(pos1, pos3);
    else if (pos1 > -1 && pos2 > -1 && pos3 > -1) {
      endPos = Math.min(pos1, pos2);
      endPos = Math.min(endPos, pos3);
    } else
      endPos = -1;
    return endPos;
  }

  protected void parseIndexSearchResult(final Collection<ODocument> entries) {
    for (final ODocument document : entries) {
      final boolean continueResultParsing = handleResult(document, false);
      if (!continueResultParsing)
        break;
    }
  }

  /**
   * Parses the fetchplan keyword if found.
   */
  protected boolean parseFetchplan(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_FETCHPLAN))
      return false;

    parserSkipWhiteSpaces();
    int start = parserGetCurrentPosition();

    parserNextWord(true);
    int end = parserGetCurrentPosition();
    parserSkipWhiteSpaces();

    int position = parserGetCurrentPosition();
    while (!parserIsEnded()) {
      parserNextWord(true);

      final String word = OStringSerializerHelper.getStringContent(parserGetLastWord());
      if (!word.matches(".*:-?\\d+"))
        break;

      end = parserGetCurrentPosition();
      parserSkipWhiteSpaces();
      position = parserGetCurrentPosition();
    }

    parserSetCurrentPosition(position);

    if (end < 0)
      fetchPlan = OStringSerializerHelper.getStringContent(parserText.substring(start));
    else
      fetchPlan = OStringSerializerHelper.getStringContent(parserText.substring(start, end));

    request.setFetchPlan(fetchPlan);

    return true;
  }

  protected boolean optimizeExecution() {
    if ((compiledFilter == null || (compiledFilter != null && compiledFilter.getRootCondition() == null)) && groupByFields == null
        && projections != null && projections.size() == 1) {

      final long startOptimization = System.currentTimeMillis();
      try {

        final Map.Entry<String, Object> entry = projections.entrySet().iterator().next();

        if (entry.getValue() instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
          if (rf.function instanceof OSQLFunctionCount && rf.configuredParameters.length == 1
              && "*".equals(rf.configuredParameters[0])) {
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

      } finally {
        context.setVariable("optimizationElapsed", (System.currentTimeMillis() - startOptimization));
      }
    }

    return false;
  }

  private int getQueryFetchLimit() {
    final int sqlLimit;
    final int requestLimit;

    if (limit > -1)
      sqlLimit = limit;
    else
      sqlLimit = -1;

    if (request.getLimit() > -1)
      requestLimit = request.getLimit();
    else
      requestLimit = -1;

    if (sqlLimit == -1)
      return requestLimit;

    if (requestLimit == -1)
      return sqlLimit;

    return Math.min(sqlLimit, requestLimit);
  }

  @SuppressWarnings("rawtypes")
  private boolean searchForIndexes(final OClass iSchemaClass) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iSchemaClass.getName().toLowerCase());

    // Create set that is sorted by amount of fields in OIndexSearchResult items
    // so the most specific restrictions will be processed first.
    final List<OIndexSearchResult> indexSearchResults = new ArrayList<OIndexSearchResult>();

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null) {
      if (orderedFields.size() == 0)
        return false;

      // use index to order documents by provided fields
      final List<String> fieldNames = new ArrayList<String>();

      for (OPair<String, String> pair : orderedFields)
        fieldNames.add(pair.getKey());

      final Set<OIndex<?>> indexes = iSchemaClass.getInvolvedIndexes(fieldNames);

      for (OIndex<?> index : indexes) {
        if (canBeUsedByOrderBy(index)) {
          final boolean ascSortOrder = orderedFields.get(0).getValue().equals(KEYWORD_ASC);
          final OQueryOperator.IndexResultListener resultListener = new IndexResultListener(fetchLimit, skip);

          if (ascSortOrder) {
            final Object firstKey = index.getFirstKey();
            if (firstKey == null)
              return false;

            index.getValuesMajor(firstKey, true, true, resultListener);
          } else {
            final Object lastKey = index.getLastKey();
            if (lastKey == null)
              return false;

            index.getValuesMinor(lastKey, true, false, resultListener);
          }

          fillSearchIndexResultSet(resultListener.getResult());

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

          return true;
        }
      }

      if (context.isRecordingMetrics()) {
        context.setVariable("indexIsUsedInOrderBy", false);
        context.setVariable("fullySortedByIndex", fullySortedByIndex);
      }
      return false;
    }

    analyzeQueryBranch(iSchemaClass, compiledFilter.getRootCondition(), indexSearchResults);

    // most specific will be processed first
    Collections.sort(indexSearchResults, new Comparator<OIndexSearchResult>() {
      public int compare(final OIndexSearchResult searchResultOne, final OIndexSearchResult searchResultTwo) {
        return searchResultTwo.getFieldCount() - searchResultOne.getFieldCount();
      }
    });

    // go through all variants to choose which one can be used for index search.
    for (final OIndexSearchResult searchResult : indexSearchResults) {
      final List<OIndex<?>> involvedIndexes = getInvolvedIndexes(iSchemaClass, searchResult);

      Collections.sort(involvedIndexes, new IndexComparator());

      // go through all possible index for given set of fields.
      for (final OIndex index : involvedIndexes) {
        if (index.isRebuiding())
          continue;

        final OIndexDefinition indexDefinition = index.getDefinition();
        final OQueryOperator operator = searchResult.lastOperator;

        // we need to test that last field in query subset and field in index that has the same position
        // are equals.
        if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
          final String lastFiled = searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
          final String relatedIndexField = indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
          if (!lastFiled.equals(relatedIndexField))
            continue;
        }

        final int searchResultFieldsCount = searchResult.fields().size();
        final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
        // We get only subset contained in processed sub query.
        for (final String fieldName : indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
          final Object fieldValue = searchResult.fieldValuePairs.get(fieldName);
          if (fieldValue instanceof OSQLQuery<?>)
            return false;

          if (fieldValue != null)
            keyParams.add(fieldValue);
          else {
            if (searchResult.lastValue instanceof OSQLQuery<?>)
              return false;

            keyParams.add(searchResult.lastValue);
          }
        }

        if (context.isRecordingMetrics()) {
          Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
          if (idxNames == null) {
            idxNames = new HashSet<String>();
            context.setVariable("involvedIndexes", idxNames);
          }
          if (index instanceof OChainedIndexProxy) {
            idxNames.addAll(((OChainedIndexProxy) index).getIndexNames());
          } else
            idxNames.add(index.getName());
        }

        Object result;
        final boolean indexIsUsedInOrderBy = canBeUsedByOrderBy(index) && !(index.getInternal() instanceof OChainedIndexProxy);
        try {
          boolean ascSortOrder;
          if (indexIsUsedInOrderBy)
            ascSortOrder = orderedFields.get(0).getValue().equals(KEYWORD_ASC);
          else
            ascSortOrder = true;

          if (indexIsUsedInOrderBy)
            fullySortedByIndex = indexDefinition.getFields().size() >= orderedFields.size();

          OQueryOperator.IndexResultListener resultListener;
          if (fetchLimit < 0 && orderedFields.isEmpty())
            resultListener = null;
          else
            resultListener = new IndexResultListener(fullySortedByIndex ? fetchLimit : (orderedFields.isEmpty() ? fetchLimit : -1),
                skip);

          result = operator.executeIndexQuery(context, index, keyParams, ascSortOrder, resultListener, fetchLimit);
        } catch (Exception e) {
          OLogManager
              .instance()
              .error(
                  this,
                  "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                  e, index.getName(), request != null && request.getText() != null ? request.getText() : "");

          fullySortedByIndex = false;
          return false;
        }

        if (result == null)
          continue;

        if (context.isRecordingMetrics()) {
          context.setVariable("indexIsUsedInOrderBy", indexIsUsedInOrderBy);
          context.setVariable("fullySortedByIndex", fullySortedByIndex);
        }

        fillSearchIndexResultSet(result);

        return true;
      }
    }
    return false;
  }

  private boolean canBeUsedByOrderBy(OIndex<?> index) {
    if (orderedFields.isEmpty())
      return false;

    if (!index.supportsOrderedIterations())
      return false;

    final OIndexDefinition definition = index.getDefinition();
    final List<String> fields = definition.getFields();
    final int endIndex = Math.min(fields.size(), orderedFields.size());

    final String firstOrder = orderedFields.get(0).getValue();
    for (int i = 0; i < endIndex; i++) {
      final OPair<String, String> pair = orderedFields.get(i);

      if (!firstOrder.equals(pair.getValue()))
        return false;

      final String orderFieldName = orderedFields.get(i).getKey().toLowerCase();
      final String indexFieldName = fields.get(i).toLowerCase();

      if (!orderFieldName.equals(indexFieldName))
        return false;
    }

    return true;
  }

  private void fillSearchIndexResultSet(final Object indexResult) {
    if (indexResult != null) {
      if (indexResult instanceof Collection<?>) {
        Collection<OIdentifiable> indexResultSet = (Collection<OIdentifiable>) indexResult;

        context.updateMetric("indexReads", indexResultSet.size());

        for (OIdentifiable identifiable : indexResultSet) {
          ORecord<?> record = identifiable.getRecord();
          // Don't throw exceptions is record is null, as indexed queries may fail when using record level security
          if ((record != null) && filter((ORecordInternal<?>) record)) {
            final boolean continueResultParsing = handleResult(record, false);
            if (!continueResultParsing)
              break;
          }
        }
      } else {
        final ORecord<?> record = ((OIdentifiable) indexResult).getRecord();
        if (filter((ORecordInternal<?>) record))
          handleResult(record, true);
      }
    }
  }

  private void applyOrderBy() {
    if (orderedFields.isEmpty() || fullySortedByIndex)
      return;

    final long startOrderBy = System.currentTimeMillis();
    try {

      if (tempResult instanceof OMultiCollectionIterator) {
        final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
        for (OIdentifiable o : tempResult)
          list.add(o);
        tempResult = list;
      }

      ODocumentHelper.sort((List<? extends OIdentifiable>) tempResult, orderedFields);
      orderedFields.clear();

    } finally {
      context.setVariable("orderByElapsed", (System.currentTimeMillis() - startOrderBy));
    }
  }

  /**
   * Extract the content of collections and/or links and put it as result
   */
  private void applyExpand() {
    if (expandTarget == null)
      return;

    Object fieldValue;

    final long startExpand = System.currentTimeMillis();
    try {

      if (tempResult == null) {
        tempResult = new ArrayList<OIdentifiable>();
        if (expandTarget instanceof OSQLFilterItemVariable) {
          Object r = ((OSQLFilterItemVariable) expandTarget).getValue(null, null, context);
          if (r != null) {
            if (r instanceof OIdentifiable)
              ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) r);
            else if (OMultiValue.isMultiValue(r)) {
              for (Object o : OMultiValue.getMultiValueIterable(r))
                ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) o);
            }
          }
        }
      } else {
        final OMultiCollectionIterator<OIdentifiable> finalResult = new OMultiCollectionIterator<OIdentifiable>();
        finalResult.setLimit(limit);
        for (OIdentifiable id : tempResult) {
          if (expandTarget instanceof OSQLFilterItem)
            fieldValue = ((OSQLFilterItem) expandTarget).getValue(id.getRecord(), null, context);
          else if (expandTarget instanceof OSQLFunctionRuntime)
            fieldValue = ((OSQLFunctionRuntime) expandTarget).getResult();
          else
            fieldValue = expandTarget.toString();

          if (fieldValue != null)
            if (fieldValue instanceof Collection<?>) {
              finalResult.add((Collection<OIdentifiable>) fieldValue);
            } else if (fieldValue instanceof Map<?, ?>) {
              finalResult.add(((Map<?, OIdentifiable>) fieldValue).values());
            } else if (fieldValue instanceof OMultiCollectionIterator) {
              finalResult.add((OMultiCollectionIterator<OIdentifiable>) fieldValue);
            } else if (fieldValue instanceof OIdentifiable)
              finalResult.add((OIdentifiable) fieldValue);
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

    if (index == null)
      throw new OCommandExecutionException("Target index '" + parsedTarget.getTargetIndex() + "' not found");

    // nothing was added yet, so index definition for manual index was not calculated
    if (index.getDefinition() == null)
      return;

    if (compiledFilter != null && compiledFilter.getRootCondition() != null) {
      if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
        throw new OCommandExecutionException("'Key' field is required for queries against indexes");

      final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();
      if (indexOperator instanceof OQueryOperatorBetween) {
        final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesBetween(getIndexKey(index.getDefinition(), values[0], context),
            getIndexKey(index.getDefinition(), values[2], context));

        for (final OIdentifiable r : entries) {
          final boolean continueResultParsing = handleResult(r, false);
          if (!continueResultParsing)
            break;
        }

      } else if (indexOperator instanceof OQueryOperatorMajor) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMajor(getIndexKey(index.getDefinition(), value, context), false);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMajorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMajor(getIndexKey(index.getDefinition(), value, context), true);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMinor) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMinor(getIndexKey(index.getDefinition(), value, context), false);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMinorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMinor(getIndexKey(index.getDefinition(), value, context), true);

        parseIndexSearchResult(entries);
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

        final Collection<ODocument> entries = index.getEntries(values);

        parseIndexSearchResult(entries);
      } else {
        final Object right = compiledFilter.getRootCondition().getRight();
        Object keyValue = getIndexKey(index.getDefinition(), right, context);
        if (keyValue == null)
          return;

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
              && ((OCompositeKey) secondKey).getKeys().size() == index.getDefinition().getParamCount())
            res = index.get(keyValue);
          else
            res = index.getValuesBetween(keyValue, secondKey, true);
        }

        if (res != null)
          if (res instanceof Collection<?>)
            // MULTI VALUES INDEX
            for (final OIdentifiable r : (Collection<OIdentifiable>) res)
              handleResult(createIndexEntryAsDocument(keyValue, r.getIdentity()), true);
          else
            // SINGLE VALUE INDEX
            handleResult(createIndexEntryAsDocument(keyValue, ((OIdentifiable) res).getIdentity()), true);
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
      if (indexInternal instanceof OSharedResource)
        ((OSharedResource) indexInternal).acquireExclusiveLock();

      try {
        // ADD ALL THE ITEMS AS RESULT
        for (Iterator<Entry<Object, Object>> it = index.iterator(); it.hasNext();) {
          final Entry<Object, Object> current = it.next();

          if (current.getValue() instanceof Collection<?>) {
            for (OIdentifiable identifiable : ((Set<OIdentifiable>) current.getValue()))
              if (!handleResult(createIndexEntryAsDocument(current.getKey(), identifiable.getIdentity()), true))
                break;
          } else if (!handleResult(createIndexEntryAsDocument(current.getKey(), (OIdentifiable) current.getValue()), true))
            break;
        }
      } finally {
        if (indexInternal instanceof OSharedResource)
          ((OSharedResource) indexInternal).releaseExclusiveLock();
      }
    }
  }

  private boolean isIndexSizeQuery() {
    if (!(groupedResult != null && projections.entrySet().size() == 1))
      return false;

    final Object projection = projections.values().iterator().next();
    if (!(projection instanceof OSQLFunctionRuntime))
      return false;

    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
    if (!f.getRoot().equals(OSQLFunctionCount.NAME))
      return false;

    if (!((f.configuredParameters == null || f.configuredParameters.length == 0) || (f.configuredParameters != null
        && f.configuredParameters.length == 1 && f.configuredParameters[0].equals("*"))))
      return false;

    return true;
  }

  private boolean isIndexKeySizeQuery() {
    if (!(groupedResult != null && projections.entrySet().size() == 1))
      return false;

    final Object projection = projections.values().iterator().next();
    if (!(projection instanceof OSQLFunctionRuntime))
      return false;

    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
    if (!f.getRoot().equals(OSQLFunctionCount.NAME))
      return false;

    if (!(f.configuredParameters != null && f.configuredParameters.length == 1 && f.configuredParameters[0] instanceof OSQLFunctionRuntime))
      return false;

    final OSQLFunctionRuntime fConfigured = (OSQLFunctionRuntime) f.configuredParameters[0];
    if (!fConfigured.getRoot().equals(OSQLFunctionDistinct.NAME))
      return false;

    if (!(fConfigured.configuredParameters != null && fConfigured.configuredParameters.length == 1 && fConfigured.configuredParameters[0] instanceof OSQLFilterItemField))
      return false;

    final OSQLFilterItemField field = (OSQLFilterItemField) fConfigured.configuredParameters[0];
    if (!field.getRoot().equals("key"))
      return false;

    return true;
  }

  private void handleNoTarget() {
    if (parsedTarget == null && expandTarget == null)
      // ONLY LET, APPLY TO THEM
      addResult(ORuntimeResult.createProjectionDocument(resultCount));
  }

  private void handleGroupBy() {
    if (groupedResult != null && tempResult == null) {

      final long startGroupBy = System.currentTimeMillis();
      try {

        tempResult = new ArrayList<OIdentifiable>();

        for (Entry<Object, ORuntimeResult> g : groupedResult.entrySet()) {
          if (g.getKey() != null || (groupedResult.size() == 1 && groupByFields == null)) {
            final ODocument doc = g.getValue().getResult();
            if (doc != null && !doc.isEmpty())
              ((List<OIdentifiable>) tempResult).add(doc);
          }
        }

      } finally {
        context.setVariable("groupByElapsed", (System.currentTimeMillis() - startGroupBy));
      }
    }
  }
}
