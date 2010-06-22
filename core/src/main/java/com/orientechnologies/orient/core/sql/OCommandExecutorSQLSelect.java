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
import java.util.List;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public class OCommandExecutorSQLSelect extends OCommandExecutorSQLAbstract implements ORecordBrowsingListener {
	public static final String												KEYWORD_SELECT	= "SELECT";

	protected OSQLAsynchQuery<ORecordSchemaAware<?>>	request;
	protected OSQLFilter															compiledFilter;
	protected List<String>														projections			= new ArrayList<String>();
	private List<OPair<String, String>>								orderedFields;
	private int																				resultCount;

	/**
	 * Compile the filter conditions only the first time.
	 */
	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLSelect parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

		init(iRequest.getDatabase(), iRequest.getText());

		request = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iRequest;

		int pos = extractProjections();
		// TODO: IF NO PROJECTION WHAT???
		if (pos == -1)
			return this;

		int endPosition = textUpperCase.indexOf(OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY, currentPos);
		if (endPosition == -1) {
			// NO OTHER STUFF: GET UNTIL THE END AND ASSURE TO RETURN FALSE IN ORDER TO AVOID PARSING OF CONDITIONS
			endPosition = text.length();
		}

		compiledFilter = new OSQLFilter(iRequest.getDatabase(), text.substring(pos, endPosition));
		currentPos = compiledFilter.currentPos + pos;

		extractOrderBy();

		return this;
	}

	protected void extractOrderBy() {
		if (currentPos == -1 || currentPos >= text.length())
			return;

		currentPos = OStringParser.jump(text, currentPos, " \r\n");

		final StringBuilder word = new StringBuilder();
		if (textUpperCase.length() - currentPos > OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY.length())
			word.append(textUpperCase.substring(currentPos, currentPos + OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY.length()));

		if (!OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY.equals(word.toString()))
			throw new OQueryParsingException("Expected keyword " + OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY);

		currentPos = currentPos += OCommandExecutorSQLAbstract.KEYWORD_ORDER_BY.length();

		String fieldName;
		String fieldOrdering;

		orderedFields = new ArrayList<OPair<String, String>>();
		while (currentPos != -1 && (orderedFields.size() == 0 || word.equals(","))) {
			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, false);
			if (currentPos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, currentPos);

			fieldName = word.toString();

			currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
			if (currentPos == -1)
				fieldOrdering = OCommandExecutorSQLAbstract.KEYWORD_ASC;
			else {

				if (word.toString().endsWith(",")) {
					currentPos--;
					word.deleteCharAt(word.length() - 1);
				}

				if (word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_ASC))
					fieldOrdering = OCommandExecutorSQLAbstract.KEYWORD_ASC;
				else if (word.toString().equals(OCommandExecutorSQLAbstract.KEYWORD_DESC))
					fieldOrdering = OCommandExecutorSQLAbstract.KEYWORD_DESC;
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
	}

	public List<String> getProjections() {
		return projections;
	}

	public List<OPair<String, String>> getOrderedFields() {
		return orderedFields;
	}

	public Object execute(final Object... iArgs) {
		// TODO: SUPPORTS MULTIPLE CLASSES LIKE A SQL JOIN
		String firstClass = compiledFilter.getClasses().size() > 0 ? compiledFilter.getClasses().keySet().iterator().next() : null;

		final int[] clusterIds;

		if (firstClass != null) {
			OClass cls = database.getMetadata().getSchema().getClass(firstClass.toLowerCase());
			if (cls == null)
				throw new OCommandExecutionException("Class " + firstClass + " was not found");

			database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, cls.getName());

			clusterIds = cls.getPolymorphicClusterIds();

			// CHECK PERMISSION TO ACCESS TO ALL THE CONFIGURED CLUSTERS
			for (int clusterId : clusterIds)
				database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, database.getClusterNameById(clusterId),
						clusterId);

			final List<ORecordId> resultSet = searchForIndexes(cls);

			if (resultSet.size() > 0) {
				// FOUND USING INDEXES
				for (ORecordId rid : resultSet)
					request.getResultListener().result(database.load(rid));
			} else
				// NO INDEXES: SCAN THE ENTIRE CLUSTER
				scanClusters(clusterIds);

		} else {
			String firstCluster = compiledFilter.getClusters().size() > 0 ? compiledFilter.getClusters().keySet().iterator().next()
					: null;

			if (firstCluster == null || firstCluster.length() == 0)
				throw new OCommandExecutionException("No cluster or schema class selected in query");

			if (Character.isDigit(firstCluster.charAt(0)))
				// GET THE CLUSTER NUMBER
				clusterIds = OStringSerializerHelper.splitIntArray(firstCluster);
			else
				// GET THE CLUSTER NUMBER BY THE CLASS NAME
				clusterIds = new int[] { database.getClusterIdByName(firstCluster.toLowerCase()) };

			database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, firstCluster.toLowerCase(), clusterIds[0]);

			scanClusters(clusterIds);
		}

		return null;
	}

	public boolean foreach(final ORecordInternal<?> iRecord) {
		if (filter(iRecord)) {
			resultCount++;
			request.getResultListener().result(iRecord.copy());

			if (request.getLimit() > -1 && resultCount == request.getLimit())
				// BREAK THE EXECUTION
				return false;
		}
		return true;
	}

	private List<ORecordId> searchForIndexes(final OClass iSchemaClass) {
		return analyzeQueryBranch(new ArrayList<ORecordId>(), iSchemaClass, compiledFilter.getRootCondition());
	}

	private List<ORecordId> analyzeQueryBranch(final List<ORecordId> iResultSet, final OClass iSchemaClass,
			final OSQLFilterCondition iCondition) {
		if (iCondition == null)
			return iResultSet;

		if (iCondition.getLeft() != null)
			if (iCondition.getLeft() instanceof OSQLFilterCondition)
				analyzeQueryBranch(iResultSet, iSchemaClass, (OSQLFilterCondition) iCondition.getLeft());

		if (iCondition.getRight() != null)
			if (iCondition.getRight() instanceof OSQLFilterCondition)
				analyzeQueryBranch(iResultSet, iSchemaClass, (OSQLFilterCondition) iCondition.getRight());

		searchIndexedProperty(iResultSet, iSchemaClass, iCondition, iCondition.getLeft());

		if (iResultSet.size() == 0)
			searchIndexedProperty(iResultSet, iSchemaClass, iCondition, iCondition.getRight());

		return iResultSet;
	}

	private List<ORecordId> searchIndexedProperty(final List<ORecordId> iResultSet, final OClass iSchemaClass,
			final OSQLFilterCondition iCondition, final Object iItem) {
		if (iItem == null || !(iItem instanceof OSQLFilterItemField))
			return null;

		OSQLFilterItemField item = (OSQLFilterItemField) iItem;

		final OProperty prop = iSchemaClass.getProperty(item.getName());
		if (prop != null && prop.isIndexed()) {
			// ONLY EQUALS IS SUPPORTED NOW!
			if (iCondition.getOperator() instanceof OQueryOperatorEquals) {
				final Object value = iCondition.getLeft() == iItem ? iCondition.getRight() : iCondition.getLeft();
				if (value != null) {
					final ORecordId record = prop.getIndex().get(value.toString());
					if (record != null)
						// FOUND: ADD IT
						iResultSet.add(record);
				}
			}
		}

		return iResultSet;
	}

	protected boolean filter(final ORecordInternal<?> iRecord) {
		return compiledFilter.evaluate(database, (ORecordSchemaAware<?>) iRecord);
	}

	protected int extractProjections() {
		final String textUpperCase = text.toUpperCase();

		int currentPos = 0;

		final StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(OCommandExecutorSQLSelect.KEYWORD_SELECT))
			return -1;

		int fromPosition = textUpperCase.indexOf(OCommandExecutorSQLAbstract.KEYWORD_FROM, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + OCommandExecutorSQLAbstract.KEYWORD_FROM, text, currentPos);

		String[] items = textUpperCase.substring(currentPos, fromPosition).split(",");
		if (items == null || items.length == 0)
			throw new OQueryParsingException("No projections found between " + OCommandExecutorSQLSelect.KEYWORD_SELECT + " and "
					+ OCommandExecutorSQLAbstract.KEYWORD_FROM, text, currentPos);

		for (String i : items)
			projections.add(i.trim());

		currentPos = fromPosition + OCommandExecutorSQLAbstract.KEYWORD_FROM.length() + 1;

		return currentPos;
	}

	private void scanClusters(final int[] clusterIds) {
		((OStorageLocal) database.getStorage()).browse(database.getId(), clusterIds, this, database.newInstance(), false);
	}
}
