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

import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * SQL CREATE LINK command: Transform a JOIN relationship to a physical LINK
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateLink extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE	= "CREATE";
	public static final String	KEYWORD_LINK		= "LINK";
	private static final String	KEYWORD_FROM		= "FROM";
	private static final String	KEYWORD_TO			= "TO";

	private String							destClassName;
	private String							destField;
	private String							sourceClassName;
	private String							sourceField;

	public OCommandExecutorSQLCreateLink parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_LINK))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_LINK + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_FROM))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_FROM + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		String[] parts = word.toString().split("\\.");
		if (parts.length != 2)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		sourceClassName = parts[0];
		if (sourceClassName == null)
			throw new OCommandSQLParsingException("Class not found", text, pos);
		sourceField = parts[1];

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_TO))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_TO + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		parts = word.toString().split("\\.");
		if (parts.length != 2)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		destClassName = parts[0];
		if (destClassName == null)
			throw new OCommandSQLParsingException("Class not found", text, pos);
		destField = parts[1];

		return this;
	}

	/**
	 * Execute the GRANT.
	 */
	public Object execute(final Object... iArgs) {
		if (destField == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		if (!(database instanceof ODatabaseDocumentTx))
			throw new OCommandSQLParsingException("This command supports only the database type ODatabaseDocumentTx");

		final ODatabaseDocumentTx db = (ODatabaseDocumentTx) database;

		OClass sourceClass = database.getMetadata().getSchema().getClass(sourceClassName);
		if (sourceClass == null)
			throw new OCommandExecutionException("Source class '" + sourceClassName + "' not found");

		OClass destClass = database.getMetadata().getSchema().getClass(destClassName);
		if (destClass == null)
			throw new OCommandExecutionException("Destination class '" + destClassName + "' not found");

		Object value;
		String cmd = "select from " + destClassName + " where " + destField + " = ";
		List<ODocument> result;
		long total = 0;

		// BROWSE ALL THE RECORDS OF THE SOURCE CLASS
		for (ODocument doc : db.browseClass(sourceClass.getName())) {
			value = doc.field(sourceField);

			if (value != null) {
				if (value instanceof ODocument || value instanceof ORID) {
					// ALREADY CONVERTED
				} else if (value instanceof Collection<?>) {
					// TODO
				} else {
					// SEARCH THE DESTINATION RECORD
					if (value instanceof String) {
						if (((String) value).length() == 0)
							value = null;
						else {
							value = "'" + value + "'";
							result = database.command(new OSQLSynchQuery<ODocument>(cmd + value)).execute();

							if (result == null || result.size() == 0)
								// throw new OCommandExecutionException("Can't create link because the destination record was not found in class '"
								// + destClass.getName() + "' and with the field '" + destField + "' equals to " + value);
								value = null;
							else if (result.size() > 1)
								throw new OCommandExecutionException("Can't create link because multiple records was found in class '"
										+ destClass.getName() + "' with value " + value + " in field '" + destField + "'");
							else
								value = result.get(0);
						}

						// SET THE REFERENCE
						doc.field(sourceField, value);
						doc.save();

						total++;
					}
				}
			}
		}

		if (total > 0) {
			// REMOVE THE OLD PROPERTY IF ANY
			OProperty prop = sourceClass.getProperty(sourceField);
			if (prop != null)
				sourceClass.removeProperty(sourceField);

			// CREATE THE PROPERTY
			sourceClass.createProperty(sourceField, OType.LINK, destClass);
			database.getMetadata().getSchema().save();
		}

		return total;
	}
}
