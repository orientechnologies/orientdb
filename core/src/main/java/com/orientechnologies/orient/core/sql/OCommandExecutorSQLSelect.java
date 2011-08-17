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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sort.ODocumentSorter;
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
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

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
	public static final String											KEYWORD_LIMIT					= "LIMIT";
	public static final String											KEYWORD_RANGE					= "RANGE";
	public static final String											KEYWORD_RANGE_FIRST		= "FIRST";
	public static final String											KEYWORD_RANGE_LAST		= "LAST";
	private static final String											KEYWORD_FROM_2FIND		= " " + KEYWORD_FROM + " ";

	private static ORecordId												FIRST									= new ORecordId();
	private static ORecordId												LAST									= new ORecordId();

	private OSQLAsynchQuery<ORecordSchemaAware<?>>	request;
	private OSQLFilter															compiledFilter;
	private Map<String, Object>											projections						= null;
	private List<OPair<String, String>>							orderedFields;
	private List<OIdentifiable>											tempResult;
	private int																			resultCount;
	private ORecordId																rangeFrom							= FIRST;
	private ORecordId																rangeTo								= LAST;
	private Object																	flattenTarget;
	private boolean																	anyFunctionAggregates	= false;

	private static final class OSearchInIndexTriple {
		private OQueryOperator	indexOperator;
		private Object					key;
		private OIndex<?>				index;

		private OSearchInIndexTriple(final OQueryOperator indexOperator, final Object key, final OIndex<?> index) {
			this.indexOperator = indexOperator;
			this.key = key;
			this.index = index;
		}
	}

	/**
	 * Compile the filter conditions only the first time.
	 */
	public OCommandExecutorSQLSelect parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

		init(iRequest.getDatabase(), iRequest.getText());

		if (iRequest instanceof OSQLSynchQuery) {
			request = (OSQLSynchQuery<ORecordSchemaAware<?>>) iRequest;
			if (request.getBeginRange().isValid())
				rangeFrom = request.getBeginRange();
			if (request.getEndRange().isValid())
				rangeTo = request.getEndRange();
		} else if (iRequest instanceof OSQLAsynchQuery)
			request = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iRequest;
		else {
			// BUILD A QUERY OBJECT FROM THE COMMAND REQUEST
			request = new OSQLSynchQuery<ORecordSchemaAware<?>>(iRequest.getText());
			request.setDatabase(iRequest.getDatabase());
			if (iRequest.getResultListener() != null)
				request.setResultListener(iRequest.getResultListener());
		}

		final int pos = parseProjections();
		if (pos == -1)
			return this;

		int endPosition = textUpperCase.indexOf(" " + OCommandExecutorSQLSelect.KEYWORD_ORDER_BY, currentPos);
		if (endPosition == -1) {
			endPosition = textUpperCase.indexOf(" " + OCommandExecutorSQLSelect.KEYWORD_RANGE, currentPos);
			if (endPosition == -1) {
				endPosition = textUpperCase.indexOf(" " + OCommandExecutorSQLSelect.KEYWORD_LIMIT, currentPos);
				if (endPosition == -1) {
					// NO OTHER STUFF: GET UNTIL THE END AND ASSURE TO RETURN FALSE IN ORDER TO AVOID PARSING OF CONDITIONS
					endPosition = text.length();
				}
			}
		}

		compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(iRequest.getDatabase(), text.substring(pos, endPosition));

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
					else if (w.equals(KEYWORD_RANGE))
						parseRange(word);
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
		// TODO: SUPPORT MULTIPLE CLASSES LIKE A SQL JOIN
		compiledFilter.bindParameters(iArgs);

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

		applyOrderBy();
		applyFlatten();
		return processResult();
	}

	public boolean foreach(final ORecordInternal<?> iRecord) {
		if (filter(iRecord))
			return addResult(iRecord);

		return true;
	}

	protected boolean addResult(final OIdentifiable iRecord) {
		resultCount++;

		final OIdentifiable recordCopy = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();

		if (orderedFields != null || flattenTarget != null) {
			// ORDER BY CLAUSE: COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
			if (tempResult == null)
				tempResult = new ArrayList<OIdentifiable>();

			tempResult.add(recordCopy);
		} else
			// CALL THE LISTENER NOW
			processRecordAsResult(recordCopy);

		if (orderedFields == null && limit > -1 && resultCount >= limit || request.getLimit() > -1 && resultCount >= request.getLimit())
			// BREAK THE EXECUTION
			return false;

		return true;
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
			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, false);
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

		if (word.toString().equals(KEYWORD_RANGE))
			// GO BACK
			currentPos -= KEYWORD_RANGE.length();
	}

	protected void parseRange(final StringBuilder word) {
		int newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

		rangeFrom = extractRangeBound(word.toString());

		if (newPos == -1)
			return;

		currentPos = newPos;
		newPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);

		if (newPos == -1)
			return;

		if (!word.toString().equalsIgnoreCase("LIMIT")) {
			rangeTo = extractRangeBound(word.toString());
			currentPos = newPos;
		}
	}

	/**
	 * Extract a range bound. Allowed values are: first, last and a valid record id
	 * 
	 * @param iRangeBound
	 *          String to parse
	 * @return {@link ORecordId} instance
	 * @throws OCommandSQLParsingException
	 *           if no valid range has been found
	 */
	protected ORecordId extractRangeBound(final String iRangeBound) throws OCommandSQLParsingException {
		if (iRangeBound.equalsIgnoreCase(KEYWORD_RANGE_FIRST))
			return FIRST;
		else if (iRangeBound.equalsIgnoreCase(KEYWORD_RANGE_LAST))
			return LAST;
		else if (!iRangeBound.contains(":"))
			throw new OCommandSQLParsingException(
					"Range must contains the keyword 'first', 'last' or a valid record id in the form of <cluster-id>:<cluster-pos>. Example: RANGE 10:50, last",
					text, currentPos);

		try {
			return new ORecordId(iRangeBound);
		} catch (Exception e) {
			throw new OCommandSQLParsingException("Invalid record id setted as RANGE to. Value setted is '" + iRangeBound
					+ "' but it should be a valid record id in the form of <cluster-id>:<cluster-pos>. Example: 10:50", text, currentPos);
		}
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

	private boolean searchForIndexes(final List<ORecord<?>> iResultSet, final OClass iSchemaClass) {
		final List<OSearchInIndexTriple> searchInIndexTriples = new LinkedList<OSearchInIndexTriple>();
		analyzeQueryBranch(iSchemaClass, compiledFilter.getRootCondition(), searchInIndexTriples);

		if (searchInIndexTriples.isEmpty())
			return false;

		for (OSearchInIndexTriple indexTriple : searchInIndexTriples) {
			final OIndex<?> idx = indexTriple.index.getInternal();
			final OQueryOperator operator = indexTriple.indexOperator;
			final Object key = indexTriple.key;

			final boolean indexCanBeUsedInEqualityOperators = (idx instanceof OIndexUnique || idx instanceof OIndexNotUnique);

			if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorBetween) {
				final Object[] betweenKeys = (Object[]) key;
				fillSearchIndexResultSet(iResultSet,
						indexTriple.index.getValuesBetween(OSQLHelper.getValue(betweenKeys[0]), OSQLHelper.getValue(betweenKeys[2])));
				return true;
			}

			if ((indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorEquals) || idx instanceof OIndexFullText
					&& operator instanceof OQueryOperatorContainsText) {
				fillSearchIndexResultSet(iResultSet, indexTriple.index.get(key));
				return true;
			}

			if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorMajor) {
				fillSearchIndexResultSet(iResultSet, idx.getValuesMajor(key, false));
				return true;
			}

			if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorMajorEquals) {
				fillSearchIndexResultSet(iResultSet, idx.getValuesMajor(key, true));
				return true;
			}

			if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorMinor) {
				fillSearchIndexResultSet(iResultSet, idx.getValuesMinor(key, false));
				return true;
			}

			if (indexCanBeUsedInEqualityOperators && operator instanceof OQueryOperatorMinorEquals) {
				fillSearchIndexResultSet(iResultSet, idx.getValuesMinor(key, true));
				return true;
			}
		}
		return false;
	}

	private void analyzeQueryBranch(final OClass iSchemaClass, final OSQLFilterCondition iCondition,
			final List<OSearchInIndexTriple> iSearchInIndexTriples) {
		if (iCondition == null)
			return;

		final OQueryOperator operator = iCondition.getOperator();
		if (operator == null)
			if (iCondition.getLeft() != null && iCondition.getRight() == null) {
				analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getLeft(), iSearchInIndexTriples);
				return;
			} else {
				return;
			}

		final OIndexReuseType indexReuseType = operator.getIndexReuseType(iCondition.getLeft(), iCondition.getRight());
		if (indexReuseType.equals(OIndexReuseType.ANY_INDEX)) {
			analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getLeft(), iSearchInIndexTriples);
			analyzeQueryBranch(iSchemaClass, (OSQLFilterCondition) iCondition.getRight(), iSearchInIndexTriples);
		} else if (indexReuseType.equals(OIndexReuseType.INDEX_METHOD)) {
			if (!searchIndexedProperty(iSchemaClass, iCondition, iCondition.getLeft(), iSearchInIndexTriples))
				searchIndexedProperty(iSchemaClass, iCondition, iCondition.getRight(), iSearchInIndexTriples);
		}
	}

	/**
	 * Searches a value in index if the property defines it.
	 * 
	 * @param iSchemaClass
	 *          Schema class
	 * @param iCondition
	 *          Condition item
	 * @param iItem
	 *          Value to search
	 * @return true if the property was indexed and found, otherwise false
	 */
	private boolean searchIndexedProperty(OClass iSchemaClass, final OSQLFilterCondition iCondition, final Object iItem,
			final List<OSearchInIndexTriple> iSearchInIndexTriples) {
		if (iItem == null || !(iItem instanceof OSQLFilterItemField))
			return false;

		if (iCondition.getLeft() instanceof OSQLFilterItemField && iCondition.getRight() instanceof OSQLFilterItemField)
			return false;

		final OSQLFilterItemField item = (OSQLFilterItemField) iItem;

		OProperty prop = iSchemaClass.getProperty(item.getRoot());

		while ((prop == null || !prop.isIndexed()) && iSchemaClass.getSuperClass() != null) {
			iSchemaClass = iSchemaClass.getSuperClass();
			prop = iSchemaClass.getProperty(item.getRoot());
		}

		if (prop != null && prop.isIndexed()) {
			final Object origValue = iCondition.getLeft() == iItem ? iCondition.getRight() : iCondition.getLeft();
			final OIndex<?> underlyingIndex = prop.getIndex().getUnderlying();

			if (iCondition.getOperator() instanceof OQueryOperatorBetween) {
				iSearchInIndexTriples.add(new OSearchInIndexTriple(iCondition.getOperator(), origValue, underlyingIndex));
				return true;
			}

			Object value = OSQLHelper.getValue(origValue);

			if (value == null)
				return false;

			value = OType.convert(value, underlyingIndex.getKeyType().getDefaultJavaType());

			if (value == null)
				return false;

			iSearchInIndexTriples.add(new OSearchInIndexTriple(iCondition.getOperator(), value, underlyingIndex));
			return true;
		}

		return false;
	}

	/**
	 * Copies or loads by their {@link ORID}s records that are returned from property index in {@link #searchIndexedProperty} method
	 * to the search result.
	 * 
	 * @param resultSet
	 *          Search result.
	 * @param indexResultSet
	 *          Result of index search.
	 */
	private void fillSearchIndexResultSet(final List<ORecord<?>> resultSet, final Object indexResult) {
		if (indexResult != null) {
			if (indexResult instanceof Collection<?>) {
				Collection<OIdentifiable> indexResultSet = (Collection<OIdentifiable>) indexResult;
				if (indexResultSet.size() > 0)
					for (OIdentifiable o : indexResultSet)
						fillResultSet(resultSet, o);

			} else
				fillResultSet(resultSet, (OIdentifiable) indexResult);

		}
	}

	private void fillResultSet(final List<ORecord<?>> resultSet, OIdentifiable o) {
		if (rangeFrom != FIRST && o.getIdentity().compareTo(rangeFrom) <= 0)
			return;

		if (rangeTo != LAST && o.getIdentity().compareTo(rangeTo) > 0)
			return;

		resultSet.add(o.getRecord());
	}

	protected boolean filter(final ORecordInternal<?> iRecord) {
		return compiledFilter.evaluate(database, (ORecordSchemaAware<?>) iRecord);
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
			int pos;
			for (String projection : items) {
				projection = projection.trim();

				if (projections == null)
					throw new OCommandSQLParsingException("Projection not allowed with FLATTEN() operator");

				fieldName = null;
				pos = projection.toUpperCase().indexOf(KEYWORD_AS);
				if (pos > -1) {
					// EXTRACT ALIAS
					fieldName = projection.substring(pos + KEYWORD_AS.length()).trim();
					projection = projection.substring(0, pos).trim();

					if (projections.containsKey(fieldName))
						throw new OCommandSQLParsingException("Field '" + fieldName
								+ "' is duplicated in current SELECT, choose a different name");
				} else {
					// EXTRACT THE FIELD NAME WITHOUT FUNCTIONS AND/OR LINKS
					final int pos1 = projection.indexOf('.');
					final int pos2 = projection.indexOf('(');

					pos = -1;

					if (pos1 > -1 && pos2 == -1)
						pos = pos1;
					else if (pos2 > -1 && pos1 == -1)
						pos = pos2;
					else if (pos1 > -1 && pos2 > -1)
						pos = Math.min(pos1, pos2);

					fieldName = pos > -1 ? projection.substring(0, pos) : projection;

					fieldName = OSQLHelper.stringContent(fieldName);

					// FIND A UNIQUE NAME BY ADDING A COUNTER
					for (int fieldIndex = 2; projections.containsKey(fieldName); ++fieldIndex) {
						fieldName += fieldIndex;
					}
				}

				if (projection.toUpperCase().startsWith("FLATTEN(")) {
					List<String> pars = OStringSerializerHelper.getParameters(projection);
					if (pars.size() != 1)
						throw new OCommandSQLParsingException("FLATTEN operator expects the field name as parameter. Example FLATTEN( out )");
					flattenTarget = OSQLHelper.parseValue(database, this, pars.get(0).trim());

					// BY PASS THIS AS PROJECTION BUT TREAT IT AS SPECIAL
					projections = null;

					if (!anyFunctionAggregates && flattenTarget instanceof OSQLFunctionRuntime
							&& ((OSQLFunctionRuntime) flattenTarget).aggregateResults())
						anyFunctionAggregates = true;

					continue;
				}

				projectionValue = OSQLHelper.parseValue(database, this, projection);
				projections.put(fieldName, projectionValue);

				if (!anyFunctionAggregates && projectionValue instanceof OSQLFunctionRuntime
						&& ((OSQLFunctionRuntime) projectionValue).aggregateResults())
					anyFunctionAggregates = true;
			}
		}

		currentPos = fromPosition + KEYWORD_FROM.length() + 1;

		return currentPos;
	}

	private void scanEntireClusters(final int[] clusterIds) {
		final ORecordId realRangeFrom = getRealRange(clusterIds, rangeFrom);
		final ORecordId realRangeTo = getRealRange(clusterIds, rangeTo);

		((OStorageEmbedded) database.getStorage()).browse(clusterIds, realRangeFrom, realRangeTo, this,
				(ORecordInternal<?>) database.newInstance(), false);
	}

	private ORecordId getRealRange(final int[] clusterIds, final ORecordId iRange) {
		if (iRange == FIRST)
			// COMPUTE THE REAL RANGE BASED ON CLUSTERS: GET THE FIRST POSITION
			return new ORecordId(clusterIds[0], 0);
		else if (iRange == LAST)
			// COMPUTE THE REAL RANGE BASED ON CLUSTERS: GET LATEST POSITION
			return new ORecordId(clusterIds[clusterIds.length - 1], -1);
		return iRange;
	}

	private void applyOrderBy() {
		if (orderedFields == null || tempResult == null)
			return;

		ODocumentSorter.sort(tempResult, orderedFields);
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
						finalResult.add((ODocument) fieldValue);
			}

		tempResult = finalResult;
	}

	private void processRecordAsResult(final OIdentifiable iRecord) {
		if (projections != null) {
			if (projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
				// SPECIAL CASE
				request.getResultListener().result(iRecord.getIdentity());
				return;
			}

			// APPLY PROJECTIONS
			final ODocument doc = (ODocument) iRecord.getRecord();
			final ODocument result = new ODocument(database).setOrdered(true);

			boolean canExcludeResult = false;

			Object value;
			for (Entry<String, Object> projection : projections.entrySet()) {
				if (projection.getValue() instanceof OSQLFilterItemField)
					value = ((OSQLFilterItemField) projection.getValue()).getValue(doc);
				else if (projection.getValue() instanceof OSQLFunctionRuntime) {
					final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection.getValue();
					canExcludeResult = f.filterResult();
					value = f.execute(doc);
				} else
					value = projection.getValue();

				if (value != null)
					result.field(projection.getKey(), value);
			}

			if (canExcludeResult && result.isEmpty())
				// RESULT EXCLUDED FOR EMPTY RECORD
				return;

			if (!anyFunctionAggregates)
				// INVOKE THE LISTENER
				request.getResultListener().result(result);
		} else
			// INVOKE THE LISTENER
			request.getResultListener().result(iRecord);
	}

	private void searchInClasses() {
		final int[] clusterIds;
		final OClass cls = compiledFilter.getTargetClasses().keySet().iterator().next();

		database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, cls.getName());

		clusterIds = cls.getPolymorphicClusterIds();

		// CHECK PERMISSION TO ACCESS TO ALL THE CONFIGURED CLUSTERS
		for (int clusterId : clusterIds)
			database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, database.getClusterNameById(clusterId));

		final List<ORecord<?>> resultSet = new ArrayList<ORecord<?>>();
		if (searchForIndexes(resultSet, cls)) {
			OProfiler.getInstance().updateCounter("Query.indexUsage", 1);

			// FOUND USING INDEXES
			for (ORecord<?> record : resultSet) {
				if (filter((ORecordInternal<?>) record))
					addResult(record);
			}
		} else
			// NO INDEXES: SCAN THE ENTIRE CLUSTER
			scanEntireClusters(clusterIds);
	}

	private void searchInClusters() {
		final int[] clusterIds;
		String firstCluster = compiledFilter.getTargetClusters().keySet().iterator().next();

		if (firstCluster == null || firstCluster.length() == 0)
			throw new OCommandExecutionException("No cluster or schema class selected in query");

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
		for (String rec : compiledFilter.getTargetRecords()) {
			rid.fromString(rec);
			record = database.load(rid);
			foreach(record);
		}
	}

	private void searchInIndex() {
		final OIndex<Object> index = (OIndex<Object>) database.getMetadata().getIndexManager()
				.getIndex(compiledFilter.getTargetIndex());
		if (index == null)
			throw new OCommandExecutionException("Target index '" + compiledFilter.getTargetIndex() + "' not found");

		if (compiledFilter.getRootCondition() != null) {
			if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
				throw new OCommandExecutionException("'Key' field is required for queries against indexes");

			final Object right = compiledFilter.getRootCondition().getRight();
			final Object keyValue = OSQLHelper.getValue(right);

			Collection<OIdentifiable> result = null;
			final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();
			if (indexOperator instanceof OQueryOperatorBetween) {
				final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();

				if (projections != null && projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
					// SPECIAL CASE
					result = index.getValuesBetween(OSQLHelper.getValue(values[0]), OSQLHelper.getValue(values[2]));

					for (OIdentifiable e : result)
						addResult(e.getIdentity());

				} else {
					final Collection<ODocument> entries = index.getEntriesBetween(OSQLHelper.getValue(values[0]),
							OSQLHelper.getValue(values[2]));

					for (OIdentifiable r : entries)
						addResult(r);
				}

			} else if (indexOperator instanceof OQueryOperatorMajor) {
				final Object value = compiledFilter.getRootCondition().getRight();
				if (projections != null && projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
					// SPECIAL CASE
					result = index.getValuesMajor(OSQLHelper.getValue(value), false);

					for (OIdentifiable e : result)
						addResult(e.getIdentity());

				} else {
					final Collection<ODocument> entries = index.getEntriesMajor(OSQLHelper.getValue(value), false);

					for (ODocument document : entries)
						addResult(document);
				}
			} else if (indexOperator instanceof OQueryOperatorMajorEquals) {
				final Object value = compiledFilter.getRootCondition().getRight();
				if (projections != null && projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
					// SPECIAL CASE
					result = index.getValuesMajor(OSQLHelper.getValue(value), true);

					for (OIdentifiable e : result)
						addResult(e.getIdentity());

				} else {
					final Collection<ODocument> entries = index.getEntriesMajor(OSQLHelper.getValue(value), true);

					for (ODocument document : entries)
						addResult(document);
				}
			} else if (indexOperator instanceof OQueryOperatorMinor) {
				final Object value = compiledFilter.getRootCondition().getRight();
				if (projections != null && projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
					// SPECIAL CASE
					result = index.getValuesMinor(OSQLHelper.getValue(value), false);

					for (OIdentifiable e : result)
						addResult(e.getIdentity());

				} else {
					final Collection<ODocument> entries = index.getEntriesMinor(OSQLHelper.getValue(value), false);

					for (ODocument document : entries)
						addResult(document);
				}
			} else if (indexOperator instanceof OQueryOperatorMinorEquals) {
				final Object value = compiledFilter.getRootCondition().getRight();
				if (projections != null && projections.size() == 1 && projections.keySet().iterator().next().equalsIgnoreCase("@rid")) {
					// SPECIAL CASE
					result = index.getValuesMinor(OSQLHelper.getValue(value), true);

					for (OIdentifiable e : result)
						addResult(e.getIdentity());

				} else {
					final Collection<ODocument> entries = index.getEntriesMinor(OSQLHelper.getValue(value), true);

					for (ODocument document : entries)
						addResult(document);
				}
			} else {
				Object res = index.get(keyValue);

				if (res != null)
					if (res instanceof Collection<?>)
						// MULTI VALUES INDEX
						for (OIdentifiable r : (Collection<OIdentifiable>) res)
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
					for (Iterator<OIdentifiable> collIt = ((ORecordLazySet) current.getValue()).rawIterator(); collIt.hasNext();)
						addResult(createIndexEntryAsDocument(current.getKey(), collIt.next().getIdentity()));
				else
					addResult(createIndexEntryAsDocument(current.getKey(), (OIdentifiable) current.getValue()));
			}

		}

		if (anyFunctionAggregates) {
			for (Entry<String, Object> projection : projections.entrySet()) {
				if (projection.getValue() instanceof OSQLFunctionRuntime) {
					final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection.getValue();
					f.setResult(index.getSize());
				}
			}
		}
	}

	private ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
		final ODocument doc = new ODocument().setOrdered(true);
		doc.field("key", iKey);
		doc.field("rid", iValue);
		doc.unsetDirty();
		return doc;
	}

	private Object processResult() {
		if (anyFunctionAggregates) {
			// EXECUTE AGGREGATIONS
			Object value;
			final ODocument result = new ODocument(database).setOrdered(true);
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

		} else if (tempResult != null) {
			int limitIndex = 0;
			// TEMP RESULT: RETURN ALL THE RECORDS AT THE END
			for (OIdentifiable doc : tempResult) {
				// CALL THE LISTENER
				if (orderedFields != null && limit > 0) {
					if (limitIndex < limit) {
						limitIndex++;
						processRecordAsResult(doc);
					} else
						// LIMIT REACHED
						break;

				} else {
					processRecordAsResult(doc);
				}
			}
			tempResult.clear();
			tempResult = null;

		}

		if (request instanceof OSQLSynchQuery)
			return ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

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

		if (left instanceof OSQLFilterItemField & right instanceof OSQLFilterItemField) {
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
