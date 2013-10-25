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

import java.util.*;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
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
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
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
import com.orientechnologies.orient.core.sql.operator.OQueryOperator.INDEX_OPERATION_TYPE;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and builds the meta information needed by the execute().
 * If the query contains the ORDER BY clause, the results are temporary collected internally, then ordered and finally returned all
 * together to the listener.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSelect extends OCommandExecutorSQLResultsetAbstract {
  private static final String         KEYWORD_AS                        = " AS ";
  public static final String          KEYWORD_SELECT                    = "SELECT";
  public static final String          KEYWORD_ASC                       = "ASC";
  public static final String          KEYWORD_DESC                      = "DESC";
  public static final String          KEYWORD_ORDER                     = "ORDER";
  public static final String          KEYWORD_BY                        = "BY";
  public static final String          KEYWORD_GROUP                     = "GROUP";
  public static final String          KEYWORD_FETCHPLAN                 = "FETCHPLAN";
  private static final int            MIN_THRESHOLD_USE_INDEX_AS_TARGET = 100;

  private Map<String, String>         projectionDefinition              = null;
  private Map<String, Object>         projections                       = null;       // THIS HAS BEEN KEPT FOR COMPATIBILITY; BUT
                                                                                       // IT'S
                                                                                       // USED THE
                                                                                       // PROJECTIONS IN GROUPED-RESULTS
  private List<OPair<String, String>> orderedFields;
  private List<String>                groupByFields;
  private Map<Object, ORuntimeResult> groupedResult;
  private Object                      expandTarget;
  private int                         fetchLimit                        = -1;
  private OIdentifiable               lastRecord;
  private Iterator<OIdentifiable>     subIterator;
  private String                      fetchPlan;

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
          else if (w.equals(KEYWORD_SKIP))
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

  protected static OSQLFilterCondition getConditionForRidPosRange(long fromId, long toId) {

    final OSQLFilterCondition fromCondition = new OSQLFilterCondition(new OSQLFilterItemField(null,
        ODocumentHelper.ATTRIBUTE_RID_POS), new OQueryOperatorMajor(), fromId);
    final OSQLFilterCondition toCondition = new OSQLFilterCondition(
        new OSQLFilterItemField(null, ODocumentHelper.ATTRIBUTE_RID_POS), new OQueryOperatorMinorEquals(), toId);

    return new OSQLFilterCondition(fromCondition, new OQueryOperatorAnd(), toCondition);
  }

  /**
   * 
   * @return {@code ture} if any of the sql functions perform aggreagation, {@code false} otherwise
   */
  public boolean isAnyFunctionAggregates() {
    return groupedResult != null;
  }

  public boolean hasNext() {
    if (lastRecord == null)
      // GET THE NEXT
      lastRecord = next();

    // BROWSE ALL THE RECORDS
    return lastRecord != null;
  }

  public OIdentifiable next() {
    if (lastRecord != null) {
      // RETURN LATEST AND RESET IT
      final OIdentifiable result = lastRecord;
      lastRecord = null;
      return result;
    }

    if (subIterator == null) {
      if (target == null) {
        // GET THE RESULT
        executeSearch(null);
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
    }

    // RESUME THE LAST POSITION
    if (lastRecord == null && subIterator != null)
      while (subIterator.hasNext()) {
        lastRecord = subIterator.next();
        if (lastRecord != null)
          return lastRecord;
      }

    return lastRecord;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove()");
  }

  public Iterator<OIdentifiable> iterator() {
    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
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

    if (request.getResultListener() != null)
      request.getResultListener().end();
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

    final ORecordInternal<?> record = id.getRecord();

    context.updateMetric("recordReads", +1);

    if (record == null || record.getRecordType() != ODocument.RECORD_TYPE)
      // SKIP IT
      return true;

    context.updateMetric("documentReads", +1);

    if (filter(record))
      if (!handleResult(record, true))
        // END OF EXECUTION
        return false;

    return true;
  }

  protected boolean handleResult(final OIdentifiable iRecord, final boolean iCloneIt) {
    lastRecord = null;

    if (orderedFields == null && skip > 0) {
      skip--;
      return true;
    }

    if (iCloneIt)
      lastRecord = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();
    else
      lastRecord = iRecord;

    resultCount++;

    addResult(lastRecord);

    if (orderedFields == null && fetchLimit > -1 && resultCount >= fetchLimit)
      // BREAK THE EXECUTION
      return false;

    return true;
  }

  protected void addResult(OIdentifiable iRecord) {
    if (iRecord == null)
      return;

    if (projections != null || groupByFields != null && !groupByFields.isEmpty()) {
      if (groupedResult == null) {
        // APPLY PROJECTIONS IN LINE
        iRecord = ORuntimeResult.getProjectionResult(resultCount, projections, context, iRecord);
        if (iRecord == null)
          return;
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
        return;
      }
    }

    if (orderedFields == null && expandTarget == null) {
      // SEND THE RESULT INLINE
      if (request.getResultListener() != null)
        request.getResultListener().result(iRecord);

    } else {

      // COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
      if (tempResult == null)
        tempResult = new ArrayList<OIdentifiable>();
      ((Collection<OIdentifiable>) tempResult).add(iRecord);
    }
  }

  protected ORuntimeResult getProjectionGroup(final Object fieldValue) {
    ORuntimeResult group = null;

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
          // LOKUP FOR THE FIELD
          key = fieldValue;
      }

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

  private int getQueryFetchLimit() {
    if (orderedFields != null) {
      return -1;
    }

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

  public Map<String, Object> getProjections() {
    return projections;
  }

  public List<OPair<String, String>> getOrderedFields() {
    return orderedFields;
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
      else if (word.equals(KEYWORD_LIMIT) || word.equals(KEYWORD_SKIP)) {
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

  @SuppressWarnings("rawtypes")
  private boolean searchForIndexes(final OClass iSchemaClass) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iSchemaClass.getName().toLowerCase());

    // Create set that is sorted by amount of fields in OIndexSearchResult items
    // so the most specific restrictions will be processed first.
    final List<OIndexSearchResult> indexSearchResults = new ArrayList<OIndexSearchResult>();

    // fetch all possible variants of subqueries that can be used in indexes.
    if (compiledFilter == null)
      return false;

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
      Collections.sort(involvedIndexes, IndexComparator.INSTANCE);

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
          if (fieldValue != null)
            keyParams.add(fieldValue);
          else
            keyParams.add(searchResult.lastValue);
        }

        INDEX_OPERATION_TYPE opType = null;

        if (context.isRecordingMetrics()) {
          Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
          if (idxNames == null) {
            idxNames = new HashSet<String>();
            context.setVariable("involvedIndexes", idxNames);
          }
          idxNames.add(index.getName());
        }

        if (projections != null && projections.size() == 1) {
          final Object v = projections.values().iterator().next();
          if (v instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) v).getFunction() instanceof OSQLFunctionCount) {
            if (!(compiledFilter.getRootCondition().getLeft() instanceof OSQLFilterCondition || compiledFilter.getRootCondition()
                .getRight() instanceof OSQLFilterCondition))
              // OPTIMIZATION: JUST COUNT IT
              opType = INDEX_OPERATION_TYPE.COUNT;
          }
        }

        if (opType == null)
          opType = INDEX_OPERATION_TYPE.GET;

        try {
          Object result = operator.executeIndexQuery(context, index, opType, keyParams, fetchLimit);

          if (result == null)
            continue;

          if (opType == INDEX_OPERATION_TYPE.COUNT) {
            // OPTIMIZATION: EMBED THE RESULT IN A DOCUMENT AND AVOID THE CLASSIC PATH
            final String projName = projectionDefinition.keySet().iterator().next();
            projectionDefinition.clear();
            getProjectionGroup(null).applyValue(projName, result);
          } else
            fillSearchIndexResultSet(result);

          return true;
        } catch (Exception e) {
          OLogManager
              .instance()
              .error(
                  this,
                  "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                  e, index, request != null && request.getText() != null ? request.getText() : "");
          return false;
        }
      }
    }
    return false;
  }

  private static List<OIndex<?>> getInvolvedIndexes(OClass iSchemaClass, OIndexSearchResult searchResultFields) {
    final Set<OIndex<?>> involvedIndexes = iSchemaClass.getInvolvedIndexes(searchResultFields.fields());

    final List<OIndex<?>> result = new ArrayList<OIndex<?>>(involvedIndexes.size());
    for (OIndex<?> involvedIndex : involvedIndexes) {
      if (searchResultFields.lastField.isLong()) {
        result.addAll(OIndexProxy.createdProxy(involvedIndex, searchResultFields.lastField, getDatabase()));
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

  protected int parseProjections() {
    if (!parserOptionalKeyword(KEYWORD_SELECT))
      return -1;

    int upperBound = OStringSerializerHelper.getLowerIndexOf(parserTextUpperCase, parserGetCurrentPosition(), KEYWORD_FROM_2FIND,
        KEYWORD_LET_2FIND);
    if (upperBound == -1)
      // UP TO THE END
      upperBound = parserText.length();

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

        fieldName = null;
        endPos = projection.toUpperCase(Locale.ENGLISH).indexOf(KEYWORD_AS);
        if (endPos > -1) {
          // EXTRACT ALIAS
          fieldName = projection.substring(endPos + KEYWORD_AS.length()).trim();
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

  private void applyOrderBy() {
    if (orderedFields == null)
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
          Object r = ((OSQLFilterItemVariable) expandTarget).getValue(null, context);
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
            fieldValue = ((OSQLFilterItem) expandTarget).getValue(id.getRecord(), context);
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
        final Collection<ODocument> entries = index.getEntriesBetween(getIndexKey(index.getDefinition(), values[0]),
            getIndexKey(index.getDefinition(), values[2]));

        for (final OIdentifiable r : entries) {
          final boolean continueResultParsing = handleResult(r, false);
          if (!continueResultParsing)
            break;
        }

      } else if (indexOperator instanceof OQueryOperatorMajor) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMajor(getIndexKey(index.getDefinition(), value), false);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMajorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMajor(getIndexKey(index.getDefinition(), value), true);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMinor) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMinor(getIndexKey(index.getDefinition(), value), false);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorMinorEquals) {
        final Object value = compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesMinor(getIndexKey(index.getDefinition(), value), true);

        parseIndexSearchResult(entries);
      } else if (indexOperator instanceof OQueryOperatorIn) {
        final List<Object> origValues = (List<Object>) compiledFilter.getRootCondition().getRight();
        final List<Object> values = new ArrayList<Object>(origValues.size());
        for (Object val : origValues) {
          if (index.getDefinition() instanceof OCompositeIndexDefinition) {
            throw new OCommandExecutionException("Operator IN not supported yet.");
          }

          val = getIndexKey(index.getDefinition(), val);
          values.add(val);
        }

        final Collection<ODocument> entries = index.getEntries(values);

        parseIndexSearchResult(entries);
      } else {
        final Object right = compiledFilter.getRootCondition().getRight();
        Object keyValue = getIndexKey(index.getDefinition(), right);

        final Object res;
        if (index.getDefinition().getParamCount() == 1) {
          // CONVERT BEFORE SEARCH IF NEEDED
          final OType type = index.getDefinition().getTypes()[0];
          keyValue = OType.convert(keyValue, type.getDefaultJavaType());

          res = index.get(keyValue);
        } else {
          final Object secondKey = getIndexKey(index.getDefinition(), right);
          if (keyValue instanceof OCompositeKey && secondKey instanceof OCompositeKey
              && ((OCompositeKey) keyValue).getKeys().size() == index.getDefinition().getParamCount()
              && ((OCompositeKey) secondKey).getKeys().size() == index.getDefinition().getParamCount())
            res = index.get(keyValue);
          else
            res = index.getValuesBetween(keyValue, secondKey);
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

  private static Object getIndexKey(final OIndexDefinition indexDefinition, Object value) {
    if (indexDefinition instanceof OCompositeIndexDefinition) {
      if (value instanceof List) {
        final List<?> values = (List<?>) value;
        List<Object> keyParams = new ArrayList<Object>(values.size());

        for (Object o : values) {
          keyParams.add(OSQLHelper.getValue(o));
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

  protected void parseIndexSearchResult(final Collection<ODocument> entries) {
    for (final ODocument document : entries) {
      final boolean continueResultParsing = handleResult(document, false);
      if (!continueResultParsing)
        break;
    }
  }

  private static ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
    final ODocument doc = new ODocument().setOrdered(true);
    doc.field("key", iKey);
    doc.field("rid", iValue);
    doc.unsetDirty();
    return doc;
  }

  private void handleNoTarget() {
    if (parsedTarget == null)
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

  @Override
  public String getSyntax() {
    return "SELECT [<Projections>] FROM <Target> [LET <Assignment>*] [WHERE <Condition>*] [ORDER BY <Fields>* [ASC|DESC]*] [LIMIT <MaxRecords>] TIMEOUT <TimeoutInMs>";
  }

  /**
   * Parses the fetchplan keyword if found.
   */
  protected boolean parseFetchplan(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_FETCHPLAN))
      return false;

    parserNextWord(true);
    fetchPlan = OStringSerializerHelper.getStringContent(parserGetLastWord());
    request.setFetchPlan(fetchPlan);

    return true;
  }

  public String getFetchPlan() {
    return fetchPlan != null ? fetchPlan : request.getFetchPlan();
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

    if (orderedFields != null && !orderedFields.isEmpty()) {
      if (parsedTarget.getTargetClasses() != null) {
        final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();
        final OPair<String, String> orderByFirstField = orderedFields.iterator().next();
        final OProperty p = cls.getProperty(orderByFirstField.getKey());
        if (p != null) {
          final Set<OIndex<?>> involvedIndexes = cls.getInvolvedIndexes(orderByFirstField.getKey());
          if (involvedIndexes != null && !involvedIndexes.isEmpty()) {
            for (OIndex<?> idx : involvedIndexes) {
              if (idx.getKeyTypes().length == 1 && idx.supportsOrderedIterations()) {
                if (idx.getType().startsWith("UNIQUE") && idx.getKeySize() < MIN_THRESHOLD_USE_INDEX_AS_TARGET
                    || compiledFilter == null) {
                  if (orderByFirstField.getValue().equalsIgnoreCase("asc"))
                    target = (Iterator<? extends OIdentifiable>) idx.valuesIterator();
                  else
                    target = (Iterator<? extends OIdentifiable>) idx.valuesInverseIterator();

                  if (context.isRecordingMetrics()) {
                    Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
                    if (idxNames == null) {
                      idxNames = new HashSet<String>();
                      context.setVariable("involvedIndexes", idxNames);
                    }
                    idxNames.add(idx.getName());
                  }

                  orderedFields = null;

                  fetchLimit = getQueryFetchLimit();
                  break;
                }
              }
            }
          }
        }
      }
    }

    return false;
  }

  private static class IndexComparator implements Comparator<OIndex<?>> {
    private static final IndexComparator INSTANCE = new IndexComparator();

    public int compare(final OIndex<?> indexOne, final OIndex<?> indexTwo) {
      return indexOne.getDefinition().getParamCount() - indexTwo.getDefinition().getParamCount();
    }
  }
}
