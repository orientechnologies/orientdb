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

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
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
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator.INDEX_OPERATION_TYPE;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and builds the meta information needed by the execute().
 * If the query contains the ORDER BY clause, the results are temporary collected internally, then ordered and finally returned all
 * together to the listener.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSelect extends OCommandExecutorSQLResultsetAbstract {
  private static final String         KEYWORD_AS            = " AS ";
  public static final String          KEYWORD_SELECT        = "SELECT";
  public static final String          KEYWORD_ASC           = "ASC";
  public static final String          KEYWORD_DESC          = "DESC";
  public static final String          KEYWORD_ORDER         = "ORDER";
  public static final String          KEYWORD_BY            = "BY";
  public static final String          KEYWORD_ORDER_BY      = "ORDER BY";

  private Map<String, Object>         projections           = null;
  private List<OPair<String, String>> orderedFields;
  private Object                      flattenTarget;
  private boolean                     anyFunctionAggregates = false;
  private int                         fetchLimit            = -1;
  private OIdentifiable               lastRecord;
  private Iterator<OIdentifiable>     subIterator;

  /**
   * Compile the filter conditions only the first time.
   */
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    super.parse(iRequest);

    final int pos = parseProjections();
    if (pos == -1)
      return this;

    int endPosition = parserText.length();

    if (context == null)
      context = new OBasicCommandContext();

    compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(parserText.substring(pos, endPosition), context);

    optimize();

    parserSetCurrentPosition(compiledFilter.parserIsEnded() ? endPosition : compiledFilter.parserGetCurrentPosition() + pos);

    if (!parserIsEnded()) {
      parserSkipWhiteSpaces();

      while (!parserIsEnded()) {
        parserNextWord(true);

        if (!parserIsEnded()) {
          final String w = parserGetLastWord();

          if (w.equals(KEYWORD_ORDER))
            parseOrderBy(w);
          else if (w.equals(KEYWORD_LIMIT))
            parseLimit(w);
          else if (w.equals(KEYWORD_SKIP))
            parseSkip(w);
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
        applyFlatten();
        applyProjections();

        subIterator = new ArrayList<OIdentifiable>(getResult()).iterator();
        lastRecord = null;
        tempResult = null;
      } else
        subIterator = (Iterator<OIdentifiable>) target.iterator();
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
    if (!optimizeExecution()) {
      fetchLimit = getQueryFetchLimit();

      executeSearch(iArgs);
      applyFlatten();
      applyProjections();
      applyOrderBy();
      applyLimitAndSkip();
    }
    return handleResult();
  }

  protected void executeSearch(final Map<Object, Object> iArgs) {
    assignTarget(iArgs);

    if (target == null)
      // SEARCH WITHOUT USING TARGET (USUALLY WHEN INDEXES ARE INVOLVED)
      return;

    // BROWSE ALL THE RECORDS
    for (OIdentifiable id : target)
      if (!executeSearchRecord(id))
        break;
  }

  @Override
  protected boolean assignTarget(Map<Object, Object> iArgs) {
    if (!super.assignTarget(iArgs)) {
      if (compiledFilter.getTargetIndex() != null)
        searchInIndex();
      else
        throw new OQueryParsingException("No source found in query: specify class, cluster(s), index or single record(s). Use "
            + getSyntax());
    }
    return true;
  }

  protected boolean executeSearchRecord(final OIdentifiable id) {
    final ORecordInternal<?> record = id.getRecord();

    context.updateMetric("recordReads", +1);

    if (record == null || record.getRecordType() != ODocument.RECORD_TYPE)
      // SKIP IT
      return true;

    context.updateMetric("documentReads", +1);

    if (filter(record))
      if (!handleResult(record))
        // END OF EXECUTION
        return false;

    return true;
  }

  protected boolean handleResult(final OIdentifiable iRecord) {
    lastRecord = null;

    if (orderedFields == null && skip > 0) {
      skip--;
      return true;
    }

    lastRecord = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();
    lastRecord = applyProjections(lastRecord);

    resultCount++;

    addResult(lastRecord);

    if (orderedFields == null && fetchLimit > -1 && resultCount >= fetchLimit)
      // BREAK THE EXECUTION
      return false;

    return true;
  }

  protected void addResult(final OIdentifiable iRecord) {
    if (iRecord != null)
      if (anyFunctionAggregates || orderedFields != null || flattenTarget != null) {
        // ORDER BY CLAUSE: COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
        if (tempResult == null)
          tempResult = new ArrayList<OIdentifiable>();

        tempResult.add(iRecord);
      } else if (subIterator == null) {
        // CALL THE LISTENER NOW BECAUSE IS INTO BROWSING (subiterator==null)
        if (request.getResultListener() != null)
          request.getResultListener().result(iRecord);
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

  protected void parseOrderBy(final String w) {
    parserRequiredKeyword(KEYWORD_BY);

    String fieldOrdering = null;

    orderedFields = new ArrayList<OPair<String, String>>();
    while (!parserIsEnded() && (orderedFields.size() == 0 || parserGetLastSeparator() == ',')) {
      final String fieldName = parserRequiredWord(false, "Field name expected");

      parserOptionalWord(true);

      final String word = parserGetLastWord();

      if (word.length() == 0)
        // END CLAUSE: SET AS ASC BY DEFAULT
        fieldOrdering = KEYWORD_ASC;
      else if (word.equals(KEYWORD_LIMIT)) {
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
    }

    if (orderedFields.size() == 0)
      throwParsingException("Order by field set was missed. Example: ORDER BY name ASC, salary DESC");
  }

  @Override
  protected void searchInClasses() {
    final OClass cls = compiledFilter.getTargetClasses().keySet().iterator().next();

    if (searchForIndexes(cls))
      OProfiler.getInstance().updateCounter("Query.indexUsage", 1);
    else
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

        Object result = operator.executeIndexQuery(index, opType, keyParams, fetchLimit);
        if (result == null)
          continue;

        if (opType == INDEX_OPERATION_TYPE.COUNT) {
          // OPTIMIZATION: EMBED THE RESULT IN A DOCUMENT AND AVOID THE CLASSIC PATH
          final String projName = projections.keySet().iterator().next();
          projections.clear();
          anyFunctionAggregates = false;
          addResult(new ODocument().field(projName, result));
        } else
          fillSearchIndexResultSet(result);

        return true;
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
          if (record == null)
            throw new OException("Error during loading record with id : " + identifiable.getIdentity());

          if (filter((ORecordInternal<?>) record)) {
            final boolean continueResultParsing = handleResult(record);
            if (!continueResultParsing)
              break;
          }
        }
      } else {
        final ORecord<?> record = ((OIdentifiable) indexResult).getRecord();
        if (filter((ORecordInternal<?>) record))
          handleResult(record);
      }
    }
  }

  protected int parseProjections() {
    if (!parserOptionalKeyword(KEYWORD_SELECT))
      return -1;

    int fromPosition = parserTextUpperCase.indexOf(KEYWORD_FROM_2FIND, parserGetCurrentPosition());
    if (fromPosition == -1)
      throwParsingException("Missed " + KEYWORD_FROM);

    Object projectionValue;
    final String projectionString = parserText.substring(parserGetCurrentPosition(), fromPosition).trim();
    if (projectionString.length() > 0 && !projectionString.equals("*")) {
      // EXTRACT PROJECTIONS
      projections = new LinkedHashMap<String, Object>();
      final List<String> items = OStringSerializerHelper.smartSplit(projectionString, ',');

      String fieldName;
      int beginPos;
      int endPos;
      for (String projection : items) {
        projection = projection.trim();

        if (projections == null)
          throw new OCommandSQLParsingException("Projection not allowed with FLATTEN() operator");

        fieldName = null;
        endPos = projection.toUpperCase(Locale.ENGLISH).indexOf(KEYWORD_AS);
        if (endPos > -1) {
          // EXTRACT ALIAS
          fieldName = projection.substring(endPos + KEYWORD_AS.length()).trim();
          projection = projection.substring(0, endPos).trim();

          if (projections.containsKey(fieldName))
            throw new OCommandSQLParsingException("Field '" + fieldName
                + "' is duplicated in current SELECT, choose a different name");
        } else {
          // EXTRACT THE FIELD NAME WITHOUT FUNCTIONS AND/OR LINKS
          beginPos = projection.charAt(0) == '@' ? 1 : 0;

          endPos = extractProjectionNameSubstringEndPosition(projection);

          fieldName = endPos > -1 ? projection.substring(beginPos, endPos) : projection.substring(beginPos);

          fieldName = OStringSerializerHelper.getStringContent(fieldName);

          // FIND A UNIQUE NAME BY ADDING A COUNTER
          for (int fieldIndex = 2; projections.containsKey(fieldName); ++fieldIndex) {
            fieldName += fieldIndex;
          }
        }

        if (projection.toUpperCase(Locale.ENGLISH).startsWith("FLATTEN(")) {
          List<String> pars = OStringSerializerHelper.getParameters(projection);
          if (pars.size() != 1)
            throw new OCommandSQLParsingException("FLATTEN operator expects the field name as parameter. Example FLATTEN( out )");
          flattenTarget = OSQLHelper.parseValue(this, pars.get(0).trim(), context);

          // BY PASS THIS AS PROJECTION BUT TREAT IT AS SPECIAL
          projections = null;

          if (!anyFunctionAggregates && flattenTarget instanceof OSQLFunctionRuntime
              && ((OSQLFunctionRuntime) flattenTarget).aggregateResults())
            anyFunctionAggregates = true;

          continue;
        }

        projectionValue = OSQLHelper.parseValue(this, projection, context);
        projections.put(fieldName, projectionValue);

        if (!anyFunctionAggregates && projectionValue instanceof OSQLFunctionRuntime
            && ((OSQLFunctionRuntime) projectionValue).aggregateResults())
          anyFunctionAggregates = true;
      }
    }

    parserSetCurrentPosition(fromPosition + KEYWORD_FROM.length() + 1);

    return parserGetCurrentPosition();
  }

  protected int extractProjectionNameSubstringEndPosition(String projection) {
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

    ODocumentHelper.sort(getResult(), orderedFields);
    orderedFields.clear();
  }

  /**
   * Extract the content of collections and/or links and put it as result
   */
  private void applyFlatten() {
    if (flattenTarget == null)
      return;

    final List<OIdentifiable> finalResult = new ArrayList<OIdentifiable>();
    Object fieldValue;
    if (tempResult != null)
      for (OIdentifiable id : tempResult) {
        if (flattenTarget instanceof OSQLFilterItem)
          fieldValue = ((OSQLFilterItem) flattenTarget).getValue(id.getRecord(), context);
        else if (flattenTarget instanceof OSQLFunctionRuntime)
          fieldValue = ((OSQLFunctionRuntime) flattenTarget).getResult();
        else
          fieldValue = flattenTarget.toString();

        if (fieldValue != null)
          if (fieldValue instanceof Collection<?>) {
            for (Object o : ((Collection<?>) fieldValue)) {
              if (o instanceof OIdentifiable)
                finalResult.add(((OIdentifiable) o).getRecord());
              else if (o instanceof List) {
                List<OIdentifiable> list = (List<OIdentifiable>) o;
                for (int i = 0; i < list.size(); i++)
                  finalResult.add(list.get(i).getRecord());
              }
            }
          } else
            finalResult.add((OIdentifiable) fieldValue);
      }

    tempResult = finalResult;
  }

  private OIdentifiable applyProjections(final OIdentifiable iRecord) {
    if (projections != null) {
      // APPLY PROJECTIONS
      final ODocument doc = iRecord.getRecord();
      final ODocument result = new ODocument().setOrdered(true);

      // ASSIGN A TEMPORARY RID TO ALLOW PAGINATION IF ANY
      ((ORecordId) result.getIdentity()).clusterId = -2;
      ((ORecordId) result.getIdentity()).clusterPosition = resultCount;

      boolean canExcludeResult = false;

      Object value;
      for (Entry<String, Object> projection : projections.entrySet()) {
        final Object v = projection.getValue();

        if (v.equals("*")) {
          doc.copy(result);
          value = null;
        } else if (v.toString().startsWith("$")) {
          // RETURN A VARIABLE FROM THE CONTEXT
          value = context != null ? context.getVariable(v.toString().substring(1)) : null;
        } else if (v instanceof OSQLFilterItemField)
          value = ((OSQLFilterItemField) v).getValue(doc, null);
        else if (v instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime f = (OSQLFunctionRuntime) v;
          canExcludeResult = f.filterResult();
          value = f.execute(doc, this);
        } else
          value = v;

        if (value != null)
          result.field(projection.getKey(), value);
      }

      if (canExcludeResult && result.isEmpty())
        // RESULT EXCLUDED FOR EMPTY RECORD
        return null;

      if (anyFunctionAggregates)
        return null;

      return result;
    }

    // INVOKE THE LISTENER
    return iRecord;
  }

  private void searchInIndex() {
    final OIndex<Object> index = (OIndex<Object>) getDatabase().getMetadata().getIndexManager()
        .getIndex(compiledFilter.getTargetIndex());

    if (index == null)
      throw new OCommandExecutionException("Target index '" + compiledFilter.getTargetIndex() + "' not found");

    // nothing was added yet, so index definition for manual index was not calculated
    if (index.getDefinition() == null)
      return;

    if (compiledFilter.getRootCondition() != null) {
      if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
        throw new OCommandExecutionException("'Key' field is required for queries against indexes");

      final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();
      if (indexOperator instanceof OQueryOperatorBetween) {
        final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();
        final Collection<ODocument> entries = index.getEntriesBetween(getIndexKey(index.getDefinition(), values[0]),
            getIndexKey(index.getDefinition(), values[2]));

        for (final OIdentifiable r : entries) {
          final boolean continueResultParsing = handleResult(r);
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
        final Object keyValue = getIndexKey(index.getDefinition(), right);

        final Object res;
        if (index.getDefinition().getParamCount() == 1) {
          res = index.get(keyValue);
        } else {
          final Object secondKey = getIndexKey(index.getDefinition(), right);
          res = index.getValuesBetween(keyValue, secondKey);
        }

        if (res != null)
          if (res instanceof Collection<?>)
            // MULTI VALUES INDEX
            for (final OIdentifiable r : (Collection<OIdentifiable>) res)
              handleResult(createIndexEntryAsDocument(keyValue, r.getIdentity()));
          else
            // SINGLE VALUE INDEX
            handleResult(createIndexEntryAsDocument(keyValue, ((OIdentifiable) res).getIdentity()));
      }

    } else {
      if (isIndexSizeQuery()) {
        final Object projection = projections.values().iterator().next();
        final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
        f.setResult(index.getSize());
        return;
      }

      if (isIndexKeySizeQuery()) {
        final Object projection = projections.values().iterator().next();
        final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
        f.setResult(index.getKeySize());
        return;
      }

      final OIndexInternal<?> indexInternal = index.getInternal();
      if (indexInternal instanceof OSharedResource)
        ((OSharedResource) indexInternal).acquireExclusiveLock();

      try {
        // ADD ALL THE ITEMS AS RESULT
        for (Iterator<Entry<Object, Object>> it = index.iterator(); it.hasNext();) {
          final Entry<Object, Object> current = it.next();

          if (current.getValue() instanceof Collection<?>)
            for (OIdentifiable identifiable : ((OMVRBTreeRIDSet) current.getValue()))
              handleResult(createIndexEntryAsDocument(current.getKey(), identifiable.getIdentity()));
          else
            handleResult(createIndexEntryAsDocument(current.getKey(), (OIdentifiable) current.getValue()));
        }
      } finally {
        if (indexInternal instanceof OSharedResource)
          ((OSharedResource) indexInternal).releaseExclusiveLock();
      }
    }
  }

  private boolean isIndexSizeQuery() {
    if (!(anyFunctionAggregates && projections.entrySet().size() == 1))
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
    if (!(anyFunctionAggregates && projections.entrySet().size() == 1))
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
      final boolean continueResultParsing = handleResult(document);
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

  private void applyProjections() {
    if (anyFunctionAggregates) {
      // EXECUTE AGGREGATIONS
      Object value;
      final ODocument result = new ODocument().setOrdered(true);
      for (Entry<String, Object> projection : projections.entrySet()) {
        if (projection.getValue() instanceof OSQLFilterItemField)
          value = ((OSQLFilterItemField) projection.getValue()).getValue(result, null);
        else if (projection.getValue() instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection.getValue();
          value = f.getResult();
        } else
          value = projection.getValue();

        result.field(projection.getKey(), value);
      }

      request.getResultListener().result(result);
    }
  }

  private static boolean checkIndexExistence(OClass iSchemaClass, OIndexSearchResult result) {
    if (!iSchemaClass.areIndexed(result.fields())) {
      return false;
    }

    if (result.lastField.isLong()) {
      final int fieldCount = result.lastField.getItemCount();
      OClass oClass = iSchemaClass.getProperty(result.lastField.getItemName(0)).getLinkedClass();

      for (int i = 1; i < fieldCount; i++) {
        if (!oClass.areIndexed(result.lastField.getItemName(i))) {
          return false;
        }

        oClass = oClass.getProperty(result.lastField.getItemName(i)).getLinkedClass();
      }
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "SELECT [<Projections>] FROM <Target> [WHERE <Condition>*] [ORDER BY <Fields>* [ASC|DESC]*] [LIMIT <MaxRecords>]";
  }

  protected boolean optimizeExecution() {
    if (compiledFilter != null & compiledFilter.getRootCondition() == null && projections != null && projections.size() == 1) {
      final Map.Entry<String, Object> entry = projections.entrySet().iterator().next();

      if (entry.getValue() instanceof OSQLFunctionRuntime) {
        final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
        if (rf.function instanceof OSQLFunctionCount && rf.configuredParameters.length == 1
            && "*".equals(rf.configuredParameters[0])) {
          long count = 0;

          if (compiledFilter.getTargetClasses() != null) {
            final OClass cls = compiledFilter.getTargetClasses().keySet().iterator().next();
            count = cls.count();
          } else if (compiledFilter.getTargetClusters() != null) {
            for (String cluster : compiledFilter.getTargetClusters().keySet()) {
              count += getDatabase().countClusterElements(cluster);
            }
          } else if (compiledFilter.getTargetIndex() != null) {
            count += getDatabase().getMetadata().getIndexManager().getIndex(compiledFilter.getTargetIndex()).getSize();
          }

          if (tempResult == null)
            tempResult = new ArrayList<OIdentifiable>();
          tempResult.add(new ODocument().field(entry.getKey(), count));
          return true;
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
