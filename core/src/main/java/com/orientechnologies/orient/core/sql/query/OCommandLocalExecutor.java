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
package com.orientechnologies.orient.core.sql.query;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandInternal;
import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OCommandExecutor;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLDelete;
import com.orientechnologies.orient.core.sql.OCommandSQLInsert;
import com.orientechnologies.orient.core.sql.OCommandSQLUpdate;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public class OCommandLocalExecutor implements OCommandExecutor {
	public static final OCommandExecutor	INSTANCE	= new OCommandLocalExecutor();

	@SuppressWarnings("unchecked")
	public <R extends ORecordSchemaAware<?>> List<R> execute(final OQuery<R> iQuery, final int iLimit) {
		OSQLAsynchQuery<ORecordSchemaAware<?>> query = (OSQLAsynchQuery<ORecordSchemaAware<?>>) iQuery;
		query.parse();

		// TODO: SUPPORTS MULTIPLE CLASSES LIKE A SQL JOIN
		String firstClass = query.compiledFilter.getClasses().size() > 0 ? query.compiledFilter.getClasses().keySet().iterator().next()
				: null;

		final int[] clusterIds;

		if (firstClass != null) {
			OClass cls = query.getDatabase().getMetadata().getSchema().getClass(firstClass.toLowerCase());
			if (cls == null)
				throw new OQueryExecutionException("Class " + firstClass + " was not found");

			if (query.getRecord() instanceof ORecordSchemaAware<?>)
				((ORecordSchemaAware<?>) query.getRecord()).setClassName(cls.getName());

			clusterIds = cls.getClusterIds();
		} else {
			String firstCluster = query.compiledFilter.getClusters().size() > 0 ? query.compiledFilter.getClusters().keySet().iterator()
					.next() : null;

			if (firstCluster == null || firstCluster.length() == 0)
				throw new OQueryExecutionException("No cluster or schema class selected in query");

			if (Character.isDigit(firstCluster.charAt(0)))
				// GET THE CLUSTER NUMBER
				clusterIds = OStringSerializerHelper.splitIntArray(firstCluster);
			else {
				// GET THE CLUSTER NUMBER BY THE CLASS NAME
				clusterIds = new int[] { query.getDatabase().getClusterIdByName(firstCluster.toLowerCase()) };
			}
		}

		((OStorageLocal) query.getDatabase().getStorage()).browse(query.getDatabase().getId(), clusterIds, query, query.getRecord());
		return null;
	}

	public <R extends ORecordSchemaAware<?>> R executeFirst(OQuery<R> iQuery) {
		return null;
	}

	public Object execute(final OCommandInternal iCommand) {
		if (iCommand instanceof OCommandSQL) {
			OCommandSQL command = (OCommandSQL) iCommand;
			final String text = command.getText();

			if (text == null || text.length() == 0)
				throw new IllegalArgumentException("Invalid SQL command");

			final String textUpperCase = text.toUpperCase();

			OCommandSQLAbstract commandToDelegate = null;

			if (textUpperCase.startsWith(OSQLHelper.KEYWORD_INSERT))
				commandToDelegate = new OCommandSQLInsert(text, textUpperCase, iCommand.getDatabase());
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_UPDATE))
				commandToDelegate = new OCommandSQLUpdate(text, textUpperCase, iCommand.getDatabase());
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_DELETE))
				commandToDelegate = new OCommandSQLDelete(text, textUpperCase, iCommand.getDatabase());
			else if (textUpperCase.startsWith(OSQLHelper.KEYWORD_SELECT)) {
				// TODO: WHAT TODO WITH SELECT?
				return null;
			}

			return commandToDelegate.execute();
		}
		return null;
	}

}
