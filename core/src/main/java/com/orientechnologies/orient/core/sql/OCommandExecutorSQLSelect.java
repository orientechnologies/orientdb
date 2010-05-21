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

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public class OCommandExecutorSQLSelect extends OCommandExecutorSQLAbstract implements ORecordBrowsingListener {
	protected OSQLAsynchQuery<ORecordSchemaAware<?>>	request;
	protected OSQLFilter															compiledFilter;
	protected List<String>														projections	= new ArrayList<String>();
	protected ORecordAbstract<?>											record;
	private int																				resultCount;

	/**
	 * Compile the filter conditions only the first time.
	 */
	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLSelect parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.OPERATIONS.READ);

		init(iRequest.getDatabase(), iRequest.getText());

		request = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iRequest;

		int pos = extractProjections();
		// TODO: IF NO PROJECTION WHAT???
		if (pos == -1)
			return this;

		compiledFilter = new OSQLFilter(iRequest.getDatabase(), text.substring(pos));

		record = (ORecordAbstract<?>) database.newInstance();

		return this;
	}

	public Object execute(final Object... iArgs) {
		// TODO: SUPPORTS MULTIPLE CLASSES LIKE A SQL JOIN
		String firstClass = compiledFilter.getClasses().size() > 0 ? compiledFilter.getClasses().keySet().iterator().next() : null;

		final int[] clusterIds;

		if (firstClass != null) {
			OClass cls = database.getMetadata().getSchema().getClass(firstClass.toLowerCase());
			if (cls == null)
				throw new OCommandExecutionException("Class " + firstClass + " was not found");

			database.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.OPERATIONS.READ, cls.getName());

			if (record instanceof ORecordSchemaAware<?>)
				((ORecordSchemaAware<?>) record).setClassName(cls.getName());

			clusterIds = cls.getClusterIds();
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

			database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.OPERATIONS.READ, firstCluster.toLowerCase(), clusterIds[0]);
		}

		((OStorageLocal) database.getStorage()).browse(database.getId(), clusterIds, this, record, false);
		return null;
	}

	public boolean foreach(final ORecordInternal<?> iRecord) {

		if (filter(iRecord)) {
			resultCount++;
			request.getResultListener().result(record.copy());

			if (request.getLimit() > -1 && resultCount == request.getLimit())
				// BREAK THE EXECUTION
				return false;
		}
		return true;
	}

	protected boolean filter(final ORecordInternal<?> iRecord) {
		return compiledFilter.evaluate(database, (ORecordSchemaAware<?>) iRecord);
	}

	public List<String> getProjections() {
		return projections;
	}

	protected int extractProjections() {
		final String textUpperCase = text.toUpperCase();

		int currentPos = 0;

		StringBuilder word = new StringBuilder();

		currentPos = OSQLHelper.nextWord(text, textUpperCase, currentPos, word, true);
		if (!word.toString().equals(OSQLHelper.KEYWORD_SELECT))
			return -1;

		int fromPosition = textUpperCase.indexOf(OSQLHelper.KEYWORD_FROM, currentPos);
		if (fromPosition == -1)
			throw new OQueryParsingException("Missed " + OSQLHelper.KEYWORD_FROM, text, currentPos);

		String[] items = textUpperCase.substring(currentPos, fromPosition).split(",");
		if (items == null || items.length == 0)
			throw new OQueryParsingException("No projections found between " + OSQLHelper.KEYWORD_SELECT + " and "
					+ OSQLHelper.KEYWORD_FROM, text, currentPos);

		for (String i : items)
			projections.add(i.trim());

		currentPos = fromPosition + OSQLHelper.KEYWORD_FROM.length() + 1;

		return currentPos;
	}
}
