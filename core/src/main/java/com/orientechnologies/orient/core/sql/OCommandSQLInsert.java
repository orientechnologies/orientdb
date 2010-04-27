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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * SQL INSERT command.
 * 
 * @author luca
 * 
 */
public class OCommandSQLInsert extends OCommandSQLAbstract {

	private static final String	KEYWORD_INTO	= "INTO";
	private String							clusterName		= null;
	private String							className			= null;
	private String[]						values;
	private String[]						fields;

	protected OCommandSQLInsert(final String iText, final String iTextUpperCase, final ODatabaseRecord<?> iDatabase) {
		super(iText, iTextUpperCase, iDatabase);
	}

	@Override
	public void parse() {
		if (clusterName != null || className != null)
			// ALREADY PARSED
			return;

		int pos = textUpperCase.indexOf(KEYWORD_INTO);
		if (pos == -1)
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_INTO + " not found", text, 0);

		pos += KEYWORD_INTO.length() + 1;

		StringBuilder word = new StringBuilder();
		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid cluster/class name", text, pos);

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

		fields = OQueryHelper.getParameters(text, beginFields);
		if (fields.length == 0)
			throw new OCommandSQLParsingException("Set of fields is empty. Example: (name, surname)", text, endFields);

		pos = OSQLHelper.nextWord(text, textUpperCase, endFields + 1, word, true);
		if (pos == -1 || !word.toString().equals("VALUES"))
			throw new OCommandSQLParsingException("Missed VALUES keyword", text, endFields);

		final int beginValues = OSQLHelper.jumpWhiteSpaces(text, pos + 1);
		if (pos == -1 || text.charAt(beginValues) != '(')
			throw new OCommandSQLParsingException("Set of values is missed. Example: ('Bill', 'Stuart', 300)", text, pos);

		final int endValues = text.indexOf(")", beginValues + 1);
		if (endValues == -1)
			throw new OCommandSQLParsingException("Missed closed brace", text, beginValues);

		values = OQueryHelper.getParameters(text, beginValues);
		if (values.length == 0)
			throw new OCommandSQLParsingException("Set of values is empty. Example: ('Bill', 'Stuart', 300)", text, beginValues);

		if (values.length != fields.length)
			throw new OCommandSQLParsingException("Fields not match with values", text, beginValues);
	}

	public Object execute() {
		parse();

		int records = 0;

		// CREATE NEW DOCUMENT
		ODocument doc = className != null ? new ODocument(database, className) : new ODocument(database);

		for (int i = 0; i < fields.length; ++i) {
			doc.field(fields[i], values[i]);
		}

		doc.save(clusterName);

		return records;
	}
}
