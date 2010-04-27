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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL UPDATE command.
 * 
 * @author luca
 * 
 */
public class OCommandSQLUpdate extends OCommandSQLAbstract implements OAsynchQueryResultListener<ODocument> {
	private static final String	KEYWORD_SET		= "SET";
	private String							className			= null;
	private Map<String, Object>	fieldEntries	= new HashMap<String, Object>();
	private OQuery<ODocument>		query;
	private int									recordCount		= 0;

	public OCommandSQLUpdate(final String iText, final String iTextUpperCase, final ODatabaseRecord<ODocument> iDatabase) {
		super(iText, iTextUpperCase, iDatabase);
	}

	@Override
	public void parse() {
		if (className != null)
			// ALREADY PARSED
			return;

		StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OSQLHelper.KEYWORD_UPDATE))
			throw new OCommandSQLParsingException("Keyword " + OSQLHelper.KEYWORD_UPDATE + " not found", text, 0);

		int newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (newPos == -1)
			throw new OCommandSQLParsingException("Invalid cluster/class name", text, pos);

		pos = newPos;

		String subjectName = word.toString();

		if (subjectName.startsWith(OSQLHelper.CLASS_PREFIX))
			subjectName = subjectName.substring(OSQLHelper.CLASS_PREFIX.length());

		// CLASS
		final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
		if (cls == null)
			throw new OCommandSQLParsingException("Class " + subjectName + " not found in database", text, pos);

		className = cls.getName();

		newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_SET))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_SET + " not found", text, pos);

		String fieldName;
		String fieldValue;

		while (pos != -1 && (fieldEntries.size() == 0 || word.toString().equals(","))) {
			newPos = OSQLHelper.nextWord(text, textUpperCase, newPos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Field name expected", text, pos);
			pos = newPos;

			fieldName = word.toString();

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (pos == -1 || !word.toString().equals("="))
				throw new OCommandSQLParsingException("Character '=' was expected", text, pos);
			pos = newPos;

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
			if (pos == -1)
				throw new OCommandSQLParsingException("Value expected", text, pos);

			fieldValue = word.toString();

			if (fieldValue.endsWith(",")) {
				pos = newPos - 1;
				fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
			} else
				pos = newPos;

			// INSERT TRANSFORMED FIELD VALUE
			fieldEntries.put(fieldName, OSQLHelper.convertValue(fieldValue));

			newPos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		}

		if (fieldEntries.size() == 0)
			throw new OCommandSQLParsingException("Set of entries <field> = <value> is missed. Example: name = 'Bill', salary = 300.2",
					text, pos);

		String whereCondition = word.toString();

		if (whereCondition.equals(OSQLHelper.KEYWORD_WHERE))
			query = database.query(new OSQLAsynchQuery<ODocument>("select from " + className + text.substring(pos), this));
		else
			query = database.query(new OSQLAsynchQuery<ODocument>("select from " + className, this));

	}

	public Object execute() {
		parse();
		query.execute();
		return recordCount;
	}

	/**
	 * Update current record.
	 */
	public boolean result(final ODocument iRecord) {
		// BIND VALUES
		for (Map.Entry<String, Object> entry : fieldEntries.entrySet())
			iRecord.field(entry.getKey(), entry.getValue());

		iRecord.save();
		recordCount++;
		return true;
	}
}
