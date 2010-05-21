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

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;

/**
 * SQL INSERT command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLInsert extends OCommandExecutorSQLAbstract {
	private static final String	KEYWORD_VALUES	= "VALUES";
	private static final String	KEYWORD_INTO		= "INTO";
	private String							className				= null;
	private String[]						fieldNames;
	private Object[]						fieldValues;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLInsert parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.OPERATIONS.CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		className = null;
		fieldNames = null;
		fieldValues = null;

		StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OSQLHelper.KEYWORD_INSERT))
			throw new OCommandSQLParsingException("Keyword " + OSQLHelper.KEYWORD_INSERT + " not found", text, 0);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_INTO))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_INTO + " not found", text, 0);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid class name", text, pos);

		String subjectName = word.toString();

		if (subjectName.startsWith(OSQLHelper.CLASS_PREFIX))
			subjectName = subjectName.substring(OSQLHelper.CLASS_PREFIX.length());

		// CLASS
		final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
		if (cls == null)
			throw new OCommandSQLParsingException("Class " + subjectName + " not found in database", text, pos);

		className = cls.getName();

		final int beginFields = OSQLHelper.jumpWhiteSpaces(text, pos);
		if (beginFields == -1 || text.charAt(beginFields) != '(')
			throw new OCommandSQLParsingException("Set of fields is missed. Example: (name, surname)", text, pos);

		final int endFields = text.indexOf(")", beginFields + 1);
		if (endFields == -1)
			throw new OCommandSQLParsingException("Missed closed brace", text, beginFields);

		fieldNames = OQueryHelper.getParameters(text, beginFields);
		if (fieldNames.length == 0)
			throw new OCommandSQLParsingException("Set of fields is empty. Example: (name, surname)", text, endFields);

		pos = OSQLHelper.nextWord(text, textUpperCase, endFields + 1, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_VALUES))
			throw new OCommandSQLParsingException("Missed VALUES keyword", text, endFields);

		final int beginValues = OSQLHelper.jumpWhiteSpaces(text, pos + 1);
		if (pos == -1 || text.charAt(beginValues) != '(')
			throw new OCommandSQLParsingException("Set of values is missed. Example: ('Bill', 'Stuart', 300)", text, pos);

		final int endValues = text.indexOf(")", beginValues + 1);
		if (endValues == -1)
			throw new OCommandSQLParsingException("Missed closed brace", text, beginValues);

		String[] values = OQueryHelper.getParameters(text, beginValues);
		if (values.length == 0)
			throw new OCommandSQLParsingException("Set of values is empty. Example: ('Bill', 'Stuart', 300)", text, beginValues);

		if (values.length != fieldNames.length)
			throw new OCommandSQLParsingException("Fields not match with values", text, beginValues);

		// TRANSFORM FIELD VALUES
		fieldValues = new Object[values.length];
		for (int i = 0; i < values.length; ++i)
			fieldValues[i] = OSQLHelper.parseValue(database, this, values[i]);

		return this;
	}

	/**
	 * Execute the INSERT and return the ODocument object created.
	 */
	public Object execute(final Object... iArgs) {
		if (fieldNames == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		// CREATE NEW DOCUMENT
		ODocument doc = className != null ? new ODocument(database, className) : new ODocument(database);

		// BIND VALUES
		Object v;
		for (int i = 0; i < fieldNames.length; ++i) {
			v = fieldValues[i];

			if (v instanceof OSQLFilterItem)
				v = ((OSQLFilterItem) v).getValue(doc);

			doc.field(fieldNames[i], v);
		}

		doc.save();

		return doc;
	}
}
