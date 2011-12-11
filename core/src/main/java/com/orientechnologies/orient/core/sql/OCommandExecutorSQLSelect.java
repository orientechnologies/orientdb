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
package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexUnique;
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
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsText;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and builds the meta information needed by the execute().
 * If the query contains the ORDER BY clause, the results are temporary collected internally, then ordered and finally returned all
 * together to the listener.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSelect extends OCommandExecutorSQLAbstract implements ORecordBrowsingListener {
	private static final String											KEYWORD_AS						= " AS ";
	public static final String											KEYWORD_SELECT				= "SELECT";
	public static final String											KEYWORD_ASC						= "ASC";
	public static final String											KEYWORD_DESC					= "DESC";
	public static final String											KEYWORD_ORDER					= "ORDER";
	public static final String											KEYWORD_BY						= "BY";
	public static final String											KEYWORD_ORDER_BY			= "ORDER BY";
	private static final String											KEYWORD_FROM_2FIND		= " " + KEYWORD_FROM + " ";

	private OSQLAsynchQuery<ORecordSchemaAware<?>>	request;
	private OSQLFilter															compiledFilter;
	private Map<String, Object>											projections						= null;
	private List<OPair<String, String>>							orderedFields;
	private List<OIdentifiable>											tempResult;
	private int																			resultCount;
	private Object																	flattenTarget;
	private boolean																	anyFunctionAggregates	= false;
	private int																			fetchLimit						= -1;

	/**
	 * Presents query subset in form of field1 = "field1 value" AND field2 = "field2 value" ... AND fieldN anyOpetator "fieldN value"
	 * 
	 * Where pairs (field1, value1) ... (fieldn-1, valuen-1) are stored in {@link #fieldValuePairs} map but last pair is stored in
	 * {@link #lastField} {@link #lastValue} properties and their operator will be stored in {@link #lastOperator} property.
	 * 
	 * Such data structure is used because from composite index point of view any "field and value" pairs can be reordered to match
	 * keys order that is used in index in case all fields and values are related to each other using equals operator, but position of
	 * field - value pair that uses non equals operator cannot be changed. Actually only one non-equals operator can be used for
	 * composite index search and filed - value pair that uses this index should always be placed at last position.
	 */
	private static final class OIndexSearchResult {
		private final Map<String, Object>	fieldValuePairs	= new HashMap<String, Object>();
		private final OQueryOperator			lastOperator;
		private final String							lastField;
		private final Object							lastValue;

		private OIndexSearchResult(final OQueryOperator lastOperator, final String field, final Object value) {
			this.lastOperator = lastOperator;
			lastField = field;
			lastValue = value;
		}

		/**
		 * Combines two queries subset into one. This operation will be valid only if {@link #canBeMerged(OIndexSearchResult)} method
		 * will return <code>true</code> for the same passed in parameter.
		 * 
		 * @param searchResult
		 *          Query subset to merge.
		 * @return New instance that presents merged query.
		 */
		private OIndexSearchResult merge(final OIndexSearchResult searchResult) {
			final OQueryOperator operator;
			final OIndexSearchResult result;

			if (!(searchResult.lastOperator instanceof OQueryOperatorEquals)) {
				operator = searchResult.lastOperator;
				result = new OIndexSearchResult(operator, searchResult.lastField, searchResult.lastValue);
				result.fieldValuePairs.putAll(searchResult.fieldValuePairs);
				result.fieldValuePairs.putAll(fieldValuePairs);
				result.fieldValuePairs.put(lastField, lastValue);
			} else {
				result = new OIndexSearchResult(this.lastOperator, lastField, lastValue);
				result.fieldValuePairs.putAll(searchResult.fieldValuePairs);
				result.fieldValuePairs.putAll(fieldValuePairs);
				result.fieldValuePairs.put(searchResult.lastField, searchResult.lastValue);
			}
			return result;
		}

		/**
		 * @param searchResult
		 *          Query subset is going to be merged with given one.
		 * @return <code>true</code> if two query subsets can be merged.
		 */
		private boolean canBeMerged(final OIndexSearchResult searchResult) {
			return (lastOperator instanceof OQueryOperatorEquals) || (searchResult.lastOperator instanceof OQueryOperatorEquals);
		}

		private List<String> fields() {
			final List<String> result = new ArrayList<String>(fieldValuePairs.size() + 1);
			result.addAll(fieldValuePairs.keySet());
			result.add(lastField);
			return result;
		}

		private int getFieldCount() {
			return fieldValuePairs.size() + 1;
		}
	}

	/**
	 * Compile the filter conditions only the first time.
	 */
	public OCommandExecutorSQLSelect parse(final OCommandRequestText iRequest) {
		final ODatabaseRecord database = getDatabase();
		database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

		init(iRequest.getText());

		if (iRequest instanceof OSQLSynchQuery) {
			request = (OSQLSynchQuery<ORecordSchemaAware<?>>) iRequest;
		} else if (iRequest instanceof OSQLAsynchQuery)
			request = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iRequest;
		else {
			// BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
			request = new OSQLSynchQuery<ORecordSchemaAware<?>>(iRequest.getText());
			if (iRequest.getResultListener() != null)
				request.setResultListener(iRequest.getResultListener());
		}

		final int pos = parseProjections();
		if (pos == -1)
			return this;

		int endPosition = text.length();
		int endP = textUpperCase.indexOf(" " + OCommandExecutorSQLSelect.KEYWORD_ORDER_BY, currentPos);
		if (endP > -1 && endP < endPosition)
			endPosition = endP;

		endP = textUpperCase.indexOf(" " + OCommandExecutorSQLSelect.KEYWORD_LIMIT, currentPos);
		if (endP > -1 && endP < endPosition)
			endPosition = endP;

		compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(text.substring(pos, endPosition));

		optimize();

		currentPos = compiledFilter.currentPos < 0 ? endPosition : compiledFilter.currentPos + pos;

		if (currentPos > -1 && currentPos < text.length()) {
			currentPos = OStringParser.jump(text, currentPos, " \r\n");

			final StringBuilder word = new StringBuilder();
			String w;

			while (currentPos > -1) {
				currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

				if (currentPos > -1) {
					w = word.toString();
					if (w.equals(KEYWORD_ORDER))
						parseOrderBy(word);
					else if (w.equals(KEYWORD_LIMIT))
						parseLimit(word);
				}
			}
		}
		if (limit == 0 || limit < -1) {
			throw new IllegalArgumentException("Limit must be > 0 or = -1 (no limit)");
		}
		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		parameters = iArgs;

		// TODO: SUPPORT MULTIPLE CLASSES LIKE A SQL JOIN
		compiledFilter.bindParameters(iArgs);

		fetchLimit = getQueryFetchLimit();

		if (compiledFilter.getTargetClasses() != null)
			searchInClasses();
		else if (compiledFilter.getTargetClusters() != null)
			searchInClusters();
		else if (compiledFilter.getTargetIndex() != null)
			searchInIndex();
		else if (compiledFilter.getTargetRecords() != null)
			searchInRecords();
		else
			throw new OQueryParsingException("No source found in query: specify class, clusters or single records");

		applyFlatten();
		applyProjections();
		applyOrderBy();
		applyLimit();

		if (tempResult != null) {
			for (OIdentifiable d : tempResult)
				if (d != null)
					request.getResultListener().result(d);
		}

		if (request instanceof OSQLSynchQuery)
			return ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

		return null;
	}

	public boolean foreach(final ORecordInternal<?> iRecord) {
		if (iRecord != null)
			if (filter(iRecord))
				return addResult(iRecord);

		return true;
	}

	protected boolean addResult(final OIdentifiable iRecord) {
		resultCount++;

		OIdentifiable recordCopy = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();
		recordCopy = applyProjections(recordCopy);

		if (recordCopy != null)
			if (anyFunctionAggregates || orderedFields != null || flattenTarget != null) {
				// ORDER BY CLAUSE: COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
				if (tempResult == null)
					tempResult = new ArrayList<OIdentifiable>();

				tempResult.add(recordCopy);
			} else {
				// CALL THE LISTENER NOW
				if (request.getResultListener() != null)
					request.getResultListener().result(recordCopy);
			}

		if (fetchLimit > -1 && resultCount >= fetchLimit)
			// BREAK THE EXECUTION
			return false;

		return true;
	}

	private int getQueryFetchLimit() {
		final int sqlLimit;
		final int requestLimit;

		if (orderedFields == null && limit > -1)
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

	protected void parseOrderBy(final StringBuilder word) {
		int newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

		if (!KEYWORD_BY.equals(word.toString()))
			throw new OQueryParsingException("Expected keyword " + KEYWORD_BY);

		currentPos = newPos;

		String fieldName;
		String fieldOrdering;

		orderedFields = new ArrayList<OPair<String, String>>();
		while (currentPos != -1 && (orderedFields.size() == 0 || word.toString().equals(","))) {
			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, false, " =><");
			if (currentPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, currentPos);

			fieldName = word.toString();

			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
			if (currentPos == -1 || word.toString().equals(KEYWORD_LIMIT))
				// END/NEXT CLAUSE: SET AS ASC BY DEFAULT
				fieldOrdering = KEYWORD_ASC;
			else {

				if (word.toString().endsWith(",")) {
					currentPos--;
					word.deleteCharAt(word.length() - 1);
				}

				if (word.toString().equals(KEYWORD_ASC))
					fieldOrdering = KEYWORD_ASC;
				else if (word.toString().equals(KEYWORD_DESC))
					fieldOrdering = KEYWORD_DESC;
				else
					throw new OCommandSQLParsingException("Ordering mode '" + word
							+ "' not supported. Valid is 'ASC', 'DESC' or nothing ('ASC' by default)", text, currentPos);

				currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
			}

			orderedFields.add(new OPair<String, String>(fieldName, fieldOrdering));

			if (currentPos == -1)
				break;
		}

		if (orderedFields.size() == 0)
			throw new OCommandSQLParsingException("Order by field set was missed. Example: ORDER BY name ASC, salary DESC", text,
					currentPos);

		if (word.toString().equals(KEYWORD_LIMIT))
			// GO BACK
			currentPos -= KEYWORD_LIMIT.length();
	}

	/**
	 * Parses the limit keyword if found.
	 * 
	 * @param word
	 *          StringBuilder to parse
	 * @return
	 * @return the limit found as integer, or -1 if no limit is found. -1 means no limits.
	 * @throws OCommandSQLParsingException
	 *           if no valid limit has been found
	 */
	protected int parseLimit(final StringBuilder word) throws OCommandSQLParsingException {
		if (!word.toString().equals(KEYWORD_LIMIT))
			return -1;

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		try {
			limit = Integer.parseInt(word.toString());
		} catch (Exception e) {
			throw new OCommandSQLParsingException("Invalid LIMIT value setted to '" + word
					+ "' but it should be a valid integer. Example: LIMIT 10", text, currentPos);
		}
		return limit;
	}

	@SuppressWarnings("rawtypes")
	private boolean searchForIndexes(final OClass iSchemaClass) {
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
			final List<String> searchResultFields = searchResult.fields();

			final List<OIndex> involvedIndexes = new ArrayList<OIndex>(iSchemaClass.getInvolvedIndexes(searchResultFields));
			Collections.sort(involvedIndexes, new Comparator<OIndex>() {
				public int compare(final OIndex indexOne, final OIndex indexTwo) {
					return indexOne.getDefinition().getParamCount() - indexTwo.getDefinition().getParamCount();
				}
			});

			// go through all possible index for given set of fields.
			for (final OIndex index : involvedIndexes) {
				final OIndexDefinition indexDefinition = index.getDefinition();
				final OQueryOperator operator = searchResult.lastOperator;

				// we need to test that last field in query subset and field in index that has the same position
				// are equals.
				if (!(operator instanceof OQueryOperatorEquals)) {
					final String lastFiled = searchResult.lastField;
					final String relatedIndexField = indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
					if (!lastFiled.equals(relatedIndexField))
						continue;
				}

				final List<Object> keyParams = new ArrayList<Object>(searchResultFields.size());
				// We get only subset contained in processed sub query.
				for (final String fieldName : indexDefinition.getFields().subList(0, searchResultFields.size())) {
					final Object fieldValue = searchResult.fieldValuePairs.get(fieldName);
					if (fieldValue != null)
						keyParams.add(fieldValue);
					else
						keyParams.add(searchResult.lastValue);
				}

				final OIndex internalIndex = index.getInternal();
				final boolean indexCanBeUsedInEqualityOperators = (internalIndex instanceof OIndexUnique || internalIndex instanceof OIndexNotUnique);

				if (indexDefinition.getParamCount() == 1) {
					if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorBetween) {
						final Object[] betweenKeys = (Object[]) keyParams.get(0);

						final Object keyOne = indexDefinition.createValue(Collections.singletonList(OSQLHelper.getValue(betweenKeys[0])));
						final Object keyTwo = indexDefinition.createValue(Collections.singletonList(OSQLHelper.getValue(betweenKeys[2])));

						if (keyOne == null || keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, true);

						fillSearchIndexResultSet(result);
						return true;
					}

					if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorIn) {
						final List<Object> inParams = (List<Object>) keyParams.get(0);
						final List<Object> inKeys = new ArrayList<Object>();

						boolean containsNotCompatibleKey = false;
						for (final Object keyValue : inParams) {
							final Object key = indexDefinition.createValue(OSQLHelper.getValue(keyValue));
							if (key == null) {
								containsNotCompatibleKey = true;
								break;
							}

							inKeys.add(key);

						}
						if (containsNotCompatibleKey)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValues(inKeys, fetchLimit);
						else
							result = index.getValues(inKeys);

						fillSearchIndexResultSet(result);
						return true;
					}

					final Object key = indexDefinition.createValue(keyParams);

					if (key == null)
						continue;

					if (internalIndex instanceof OIndexFullText && operator instanceof OQueryOperatorContainsText) {
						fillSearchIndexResultSet(index.get(key));
						return true;
					}

					if (!indexCanBeUsedInEqualityOperators)
						continue;

					if (operator instanceof OQueryOperatorEquals) {
						fillSearchIndexResultSet(index.get(key));
						return true;
					}

					if (operator instanceof OQueryOperatorMajor) {
						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesMajor(key, false, fetchLimit);
						else
							result = index.getValuesMajor(key, false);

						fillSearchIndexResultSet(result);
						return true;
					}

					if (operator instanceof OQueryOperatorMajorEquals) {
						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesMajor(key, true, fetchLimit);
						else
							result = index.getValuesMajor(key, true);

						fillSearchIndexResultSet(result);
						return true;
					}

					if (operator instanceof OQueryOperatorMinor) {
						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesMinor(key, false, fetchLimit);
						else
							result = index.getValuesMinor(key, false);

						fillSearchIndexResultSet(result);
						return true;
					}

					if (operator instanceof OQueryOperatorMinorEquals) {
						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesMinor(key, true, fetchLimit);
						else
							result = index.getValuesMinor(key, true);

						fillSearchIndexResultSet(result);
						return true;
					}
				} else {
					if (!indexCanBeUsedInEqualityOperators)
						continue;

					if (operator instanceof OQueryOperatorBetween) {
						final Object[] betweenKeys = (Object[]) keyParams.get(keyParams.size() - 1);

						final Object betweenKeyOne = OSQLHelper.getValue(betweenKeys[0]);

						if (betweenKeyOne == null)
							continue;

						final Object betweenKeyTwo = OSQLHelper.getValue(betweenKeys[2]);

						if (betweenKeyTwo == null)
							continue;

						final List<Object> betweenKeyOneParams = new ArrayList<Object>(keyParams.size());
						betweenKeyOneParams.addAll(keyParams.subList(0, keyParams.size() - 1));
						betweenKeyOneParams.add(betweenKeyOne);

						final List<Object> betweenKeyTwoParams = new ArrayList<Object>(keyParams.size());
						betweenKeyTwoParams.addAll(keyParams.subList(0, keyParams.size() - 1));
						betweenKeyTwoParams.add(betweenKeyTwo);

						final Object keyOne = indexDefinition.createValue(betweenKeyOneParams);

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(betweenKeyTwoParams);

						if (keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, true);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}

						return true;
					}

					if (operator instanceof OQueryOperatorEquals) {
						// in case of composite keys several items can be returned in case of we perform search
						// using part of composite key stored in index.

						final Object keyOne = indexDefinition.createValue(keyParams);

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(keyParams);

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, true);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}
						return true;
					}

					if (operator instanceof OQueryOperatorMajor) {
						// if we have situation like "field1 = 1 AND field2 > 2"
						// then we fetch collection which left not included boundary is the smallest composite key in the
						// index that contains keys with values field1=1 and field2=2 and which right included boundary
						// is the biggest composite key in the index that contains key with value field1=1.

						final Object keyOne = indexDefinition.createValue(keyParams);

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(keyParams.subList(0, keyParams.size() - 1));

						if (keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, false, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, false, keyTwo, true);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}
						return true;
					}

					if (operator instanceof OQueryOperatorMajorEquals) {
						// if we have situation like "field1 = 1 AND field2 >= 2"
						// then we fetch collection which left included boundary is the smallest composite key in the
						// index that contains keys with values field1=1 and field2=2 and which right included boundary
						// is the biggest composite key in the index that contains key with value field1=1.

						final Object keyOne = indexDefinition.createValue(keyParams);

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(keyParams.subList(0, keyParams.size() - 1));

						if (keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, true);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}
						return true;
					}

					if (operator instanceof OQueryOperatorMinor) {
						// if we have situation like "field1 = 1 AND field2 < 2"
						// then we fetch collection which left included boundary is the smallest composite key in the
						// index that contains key with value field1=1 and which right not included boundary
						// is the biggest composite key in the index that contains key with values field1=1 and field2=2.

						final Object keyOne = indexDefinition.createValue(keyParams.subList(0, keyParams.size() - 1));

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(keyParams);

						if (keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, false, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, false);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}
						return true;
					}

					if (operator instanceof OQueryOperatorMinorEquals) {
						// if we have situation like "field1 = 1 AND field2 <= 2"
						// then we fetch collection which left included boundary is the smallest composite key in the
						// index that contains key with value field1=1 and which right not included boundary
						// is the biggest composite key in the index that contains key with value field1=1 and field2=2.

						final Object keyOne = indexDefinition.createValue(keyParams.subList(0, keyParams.size() - 1));

						if (keyOne == null)
							continue;

						final Object keyTwo = indexDefinition.createValue(keyParams);

						if (keyTwo == null)
							continue;

						final Collection<OIdentifiable> result;
						if (fetchLimit > -1)
							result = index.getValuesBetween(keyOne, true, keyTwo, true, fetchLimit);
						else
							result = index.getValuesBetween(keyOne, true, keyTwo, true);

						fillSearchIndexResultSet(result);

						if (OProfiler.getInstance().isRecording()) {
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage", 1);
							OProfiler.getInstance().updateCounter("Query.compositeIndexUsage." + indexDefinition.getParamCount(), 1);
						}
						return true;
					}
				}
			}
		}
		return false;
	}

	private OIndexSearchResult analyzeQueryBranch(final OClass iSchemaClass, final OSQLFilterCondition iCondition,
			final List<OIndexSearchResult> iIndexSearchResults) {
		if (iCondition == null)
			return null;

		final OQueryOperator operator = iCondition.getOperator();
		if (operator == null)
			if (iCondition.getRight() == null && iCondition.getLeft() instanceof OSQLFilterCondition) {
				return analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getLeft(), iIndexSearchResults);
			} else {
				return null;
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

			if (iSchemaClass.areIndexed(result.fields()))
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
	private OIndexSearchResult createIndexedProperty(final OSQLFilterCondition iCondition, final Object iItem) {
		if (iItem == null || !(iItem instanceof OSQLFilterItemField))
			return null;

		if (iCondition.getLeft() instanceof OSQLFilterItemField && iCondition.getRight() instanceof OSQLFilterItemField)
			return null;

		final OSQLFilterItemField item = (OSQLFilterItemField) iItem;

		if (item.hasChainOperators())
			return null;

		final Object origValue = iCondition.getLeft() == iItem ? iCondition.getRight() : iCondition.getLeft();

		if (iCondition.getOperator() instanceof OQueryOperatorBetween || iCondition.getOperator() instanceof OQueryOperatorIn) {
			return new OIndexSearchResult(iCondition.getOperator(), item.getRoot(), origValue);
		}

		final Object value = OSQLHelper.getValue(origValue);

		if (value == null)
			return null;

		return new OIndexSearchResult(iCondition.getOperator(), item.getRoot(), value);
	}

	@SuppressWarnings("rawtypes")
	private void fillSearchIndexResultSet(final Object indexResult) {
		if (indexResult != null) {
			if (indexResult instanceof Collection<?>) {
				Collection<OIdentifiable> indexResultSet = (Collection<OIdentifiable>) indexResult;
				if (!indexResultSet.isEmpty()) {
					// FOUND USING INDEXES
					for (OIdentifiable identifiable : indexResultSet) {
						ORecord<?> record = identifiable.getRecord();
						if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
							try {
								record = record.<ORecord> load();
							} catch (ORecordNotFoundException e) {
								throw new OException("Error during loading record with id : " + record.getIdentity());
							}
						}

						if (filter((ORecordInternal<?>) record)) {
							final boolean continueResultParsing = addResult(record);
							if (!continueResultParsing)
								break;
						}
					}
				}
			} else {
				final ORecord<?> record = ((OIdentifiable) indexResult).getRecord();
				if (filter((ORecordInternal<?>) record))
					addResult(record);
			}
		}
	}

	protected boolean filter(final ORecordInternal<?> iRecord) {
		return compiledFilter.evaluate((ORecordSchemaAware<?>) iRecord);
	}

	protected int parseProjections() {
		int currentPos = 0;
		final StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(KEYWORD_SELECT))
			return -1;

		int fromPosition = textUpperCase.indexOf(KEYWORD_FROM_2FIND, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + KEYWORD_FROM, text, currentPos);

		Object projectionValue;
		final String projectionString = text.substring(currentPos, fromPosition).trim();
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
					flattenTarget = OSQLHelper.parseValue(this, pars.get(0).trim());

					// BY PASS THIS AS PROJECTION BUT TREAT IT AS SPECIAL
					projections = null;

					if (!anyFunctionAggregates && flattenTarget instanceof OSQLFunctionRuntime
							&& ((OSQLFunctionRuntime) flattenTarget).aggregateResults())
						anyFunctionAggregates = true;

					continue;
				}

				projectionValue = OSQLHelper.parseValue(this, projection);
				projections.put(fieldName, projectionValue);

				if (!anyFunctionAggregates && projectionValue instanceof OSQLFunctionRuntime
						&& ((OSQLFunctionRuntime) projectionValue).aggregateResults())
					anyFunctionAggregates = true;
			}
		}

		currentPos = fromPosition + KEYWORD_FROM.length() + 1;

		return currentPos;
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

	private void scanEntireClusters(final int[] clusterIds) {
		final OSQLFilterCondition rootCondition = compiledFilter.getRootCondition();

		final ODatabaseRecord database = getDatabase();

		if (rootCondition == null){
      final ORID beginRange;
      if(request instanceof OSQLSynchQuery)
        beginRange = ((OSQLSynchQuery)request).getNextPageRID();
      else
        beginRange = null;

			((OStorageEmbedded) database.getStorage()).browse(clusterIds, beginRange, null, this, (ORecordInternal<?>) database.newInstance(),
					false);
    }	else
			((OStorageEmbedded) database.getStorage()).browse(clusterIds, rootCondition.getBeginRidRange(),
					rootCondition.getEndRidRange(), this, (ORecordInternal<?>) database.newInstance(), false);
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
					fieldValue = ((OSQLFilterItem) flattenTarget).getValue((ORecordInternal<?>) id.getRecord());
				else if (flattenTarget instanceof OSQLFunctionRuntime)
					fieldValue = ((OSQLFunctionRuntime) flattenTarget).getResult();
				else
					fieldValue = flattenTarget.toString();

				if (fieldValue != null)
					if (fieldValue instanceof Collection<?>) {
						for (Object o : ((Collection<?>) fieldValue)) {
							if (o instanceof ODocument)
								finalResult.add((ODocument) o);
						}
					} else
						finalResult.add((OIdentifiable) fieldValue);
			}

		tempResult = finalResult;
	}

	private OIdentifiable applyProjections(final OIdentifiable iRecord) {
		if (projections != null) {
			// APPLY PROJECTIONS
			final ODocument doc = (ODocument) iRecord.getRecord();
			final ODocument result = new ODocument().setOrdered(true);

			boolean canExcludeResult = false;

			Object value;
			for (Entry<String, Object> projection : projections.entrySet()) {
				if (projection.getValue().equals("*")) {
					doc.copy(result);
					value = null;
				} else if (projection.getValue() instanceof OSQLFilterItemField)
					value = ((OSQLFilterItemField) projection.getValue()).getValue(doc);
				else if (projection.getValue() instanceof OSQLFunctionRuntime) {
					final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection.getValue();
					canExcludeResult = f.filterResult();
					value = f.execute(doc, this);
				} else
					value = projection.getValue();

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

	private void searchInClasses() {
		final int[] clusterIds;
		final OClass cls = compiledFilter.getTargetClasses().keySet().iterator().next();

		final ODatabaseRecord database = getDatabase();
		database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, cls.getName());

		clusterIds = cls.getPolymorphicClusterIds();

		// CHECK PERMISSION TO ACCESS TO ALL THE CONFIGURED CLUSTERS
		for (final int clusterId : clusterIds)
			database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, database.getClusterNameById(clusterId));

		if (searchForIndexes(cls))
			OProfiler.getInstance().updateCounter("Query.indexUsage", 1);
		else
			// NO INDEXES: SCAN THE ENTIRE CLUSTER
			scanEntireClusters(clusterIds);
	}

	private void searchInClusters() {
		final int[] clusterIds;
		String firstCluster = compiledFilter.getTargetClusters().keySet().iterator().next();

		if (firstCluster == null || firstCluster.length() == 0)
			throw new OCommandExecutionException("No cluster or schema class selected in query");

		final ODatabaseRecord database = getDatabase();

		if (Character.isDigit(firstCluster.charAt(0)))
			// GET THE CLUSTER NUMBER
			clusterIds = OStringSerializerHelper.splitIntArray(firstCluster);
		else
			// GET THE CLUSTER NUMBER BY THE CLASS NAME
			clusterIds = new int[] { database.getClusterIdByName(firstCluster.toLowerCase()) };

		database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, firstCluster.toLowerCase());

		scanEntireClusters(clusterIds);
	}

	private void searchInRecords() {
		ORecordId rid = new ORecordId();
		ORecordInternal<?> record;
		final ODatabaseRecord database = getDatabase();
		for (String rec : compiledFilter.getTargetRecords()) {
			rid.fromString(rec);
			record = database.load(rid);
			final boolean continueResultParsing = foreach(record);
			if (!continueResultParsing)
				break;
		}
	}

	private void searchInIndex() {
		final OIndex<Object> index = (OIndex<Object>) getDatabase().getMetadata().getIndexManager()
				.getIndex(compiledFilter.getTargetIndex());
		if (index == null)
			throw new OCommandExecutionException("Target index '" + compiledFilter.getTargetIndex() + "' not found");

		if (compiledFilter.getRootCondition() != null) {
			if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
				throw new OCommandExecutionException("'Key' field is required for queries against indexes");

			final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();
			if (indexOperator instanceof OQueryOperatorBetween) {
				final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();
				final Collection<ODocument> entries = index.getEntriesBetween(getIndexKey(index.getDefinition(), values[0]),
						getIndexKey(index.getDefinition(), values[2]));

				for (final OIdentifiable r : entries) {
					final boolean continueResultParsing = addResult(r);
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
							addResult(createIndexEntryAsDocument(keyValue, r.getIdentity()));
					else
						// SINGLE VALUE INDEX
						addResult(createIndexEntryAsDocument(keyValue, ((OIdentifiable) res).getIdentity()));
			}

		} else {

			// ADD ALL THE ITEMS AS RESULT
			for (Iterator<Entry<Object, Object>> it = index.iterator(); it.hasNext();) {
				final Entry<Object, Object> current = it.next();

				if (current.getValue() instanceof Collection<?>)
					for (Iterator<OIdentifiable> collIt = ((OMVRBTreeRIDSet) current.getValue()).iterator(); collIt.hasNext();)
						addResult(createIndexEntryAsDocument(current.getKey(), collIt.next().getIdentity()));
				else
					addResult(createIndexEntryAsDocument(current.getKey(), (OIdentifiable) current.getValue()));
			}

		}

		if (anyFunctionAggregates) {
			for (final Entry<String, Object> projection : projections.entrySet()) {
				if (projection.getValue() instanceof OSQLFunctionRuntime) {
					final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection.getValue();
					f.setResult(index.getSize());
				}
			}
		}
	}

	private Object getIndexKey(final OIndexDefinition indexDefinition, Object value) {
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
			final boolean continueResultParsing = addResult(document);
			if (!continueResultParsing)
				break;
		}
	}

	private ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
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
					value = ((OSQLFilterItemField) projection.getValue()).getValue(result);
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

	private void applyLimit() {
		if (tempResult != null && limit > 0) {
			final List<OIdentifiable> newList = new ArrayList<OIdentifiable>();

			// APPLY LIMIT
			final int tot = Math.min(limit, tempResult.size());
			for (int i = 0; i < tot; ++i)
				newList.add(tempResult.get(i));

			tempResult.clear();
			tempResult = newList;
		}
	}

	public List<OIdentifiable> getResult() {
		if (tempResult != null)
			return tempResult;

		if (request instanceof OSQLSynchQuery)
			return (List<OIdentifiable>) ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

		return null;
	}

	/**
	 * Optimizes the contidion tree.
	 */
	private void optimize() {
		if (compiledFilter == null)
			return;

		optimizeBranch(null, compiledFilter.getRootCondition());
	}

	private void optimizeBranch(final OSQLFilterCondition iParentCondition, OSQLFilterCondition iCondition) {
		if (iCondition == null)
			return;

		final Object left = iCondition.getLeft();

		if (left instanceof OSQLFilterCondition)
			// ANALYSE LEFT RECURSIVELY
			optimizeBranch(iCondition, (OSQLFilterCondition) left);

		final Object right = iCondition.getRight();

		if (right instanceof OSQLFilterCondition)
			// ANALYSE RIGHT RECURSIVELY
			optimizeBranch(iCondition, (OSQLFilterCondition) right);

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
}
