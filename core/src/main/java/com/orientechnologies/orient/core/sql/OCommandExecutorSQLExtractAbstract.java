/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequestText;
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
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
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
public abstract class OCommandExecutorSQLExtractAbstract extends OCommandExecutorSQLAbstract {
	protected static final String											KEYWORD_FROM_2FIND	= " " + KEYWORD_FROM + " ";

	protected OSQLAsynchQuery<ORecordSchemaAware<?>>	request;
	protected OSQLFilter															compiledFilter;
	protected Iterable<? extends OIdentifiable>				target;
	protected List<OIdentifiable>											tempResult;
	protected int																			resultCount;

	/**
	 * Compile the filter conditions only the first time.
	 */
	public OCommandExecutorSQLExtractAbstract parse(final OCommandRequestText iRequest) {
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
		return this;
	}

	public List<OIdentifiable> getResult() {
		if (tempResult != null)
			return tempResult;

		if (request instanceof OSQLSynchQuery)
			return (List<OIdentifiable>) ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

		return null;
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

		// TODO: SUPPORT MULTIPLE CLASSES LIKE A SQL JOIN
		compiledFilter.bindParameters(iArgs);

		if (target == null)
			if (compiledFilter.getTargetClasses() != null)
				searchInClasses();
			else if (compiledFilter.getTargetClusters() != null)
				searchInClusters();
			else if (compiledFilter.getTargetRecords() != null)
				target = compiledFilter.getTargetRecords();
			else
				return false;

		return true;
	}

	protected Object handleResult() {
		if (tempResult != null) {
			for (OIdentifiable d : tempResult)
				if (d != null)
					request.getResultListener().result(d);
		}

		if (request instanceof OSQLSynchQuery)
			return ((OSQLSynchQuery<ORecordSchemaAware<?>>) request).getResult();

		return null;
	}

	protected boolean addResult(final OIdentifiable iRecord) {
		resultCount++;

		OIdentifiable recordCopy = iRecord instanceof ORecord<?> ? ((ORecord<?>) iRecord).copy() : iRecord.getIdentity().copy();

		if (recordCopy != null)
			// CALL THE LISTENER NOW
			if (request.getResultListener() != null)
				request.getResultListener().result(recordCopy);

		if (limit > -1 && resultCount >= limit)
			// BREAK THE EXECUTION
			return false;

		return true;
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

	protected boolean filter(final ORecordInternal<?> iRecord) {
		return compiledFilter.evaluate(iRecord, context);
	}

	protected void searchInClasses() {
		final OClass cls = compiledFilter.getTargetClasses().keySet().iterator().next();

		final ODatabaseRecord database = getDatabase();
		database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, cls.getName().toLowerCase());

		// NO INDEXES: SCAN THE ENTIRE CLUSTER
		final ORID[] range = getRange();
		target = new ORecordIteratorClass<ORecordInternal<?>>(database, (ODatabaseRecordAbstract) database, cls.getName(), true)
				.setRange(range[0], range[1]);
	}

	protected void searchInClusters() {
		final ODatabaseRecord database = getDatabase();

		final Set<Integer> clusterIds = new HashSet<Integer>();
		for (String clusterName : compiledFilter.getTargetClusters().keySet()) {
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

		target = new ORecordIteratorClusters<ORecordInternal<?>>(database, (ODatabaseRecordAbstract) database, clIds).setRange(
				range[0], range[1]);
	}

	protected void applyLimit() {
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

	/**
	 * Optimizes the condition tree.
	 */
	protected void optimize() {
		if (compiledFilter == null)
			return;

		optimizeBranch(null, compiledFilter.getRootCondition());
	}

	protected void optimizeBranch(final OSQLFilterCondition iParentCondition, OSQLFilterCondition iCondition) {
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

	protected ORID[] getRange() {
		final ORID beginRange;
		final ORID endRange;

		final OSQLFilterCondition rootCondition = compiledFilter.getRootCondition();
		if (rootCondition == null) {
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
